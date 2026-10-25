// Package worker pulls leased tasks from the cluster and runs them.
//
// A lease is a lock with a deadline. The pool keeps it alive with periodic
// heartbeats while a handler runs. If the heartbeat discovers the lease is
// gone (the cluster gave the task to someone else after a pause or a
// partition), the handler's context is cancelled and nothing is reported:
// the fencing token on the server would reject it anyway.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/apierr"
	"github.com/monaaoua9/dtask/internal/backoff"
)

type API interface {
	Lease(ctx context.Context, req *pb.LeaseRequest) (*pb.LeaseResponse, error)
	ExtendLease(ctx context.Context, req *pb.ExtendLeaseRequest) (*pb.ExtendLeaseResponse, error)
	Complete(ctx context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error)
	Fail(ctx context.Context, req *pb.FailRequest) (*pb.FailResponse, error)
	Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error)
}

// Handler runs one task. Returning an error makes the task retry (subject to
// its policy); wrap it with Permanent to skip the remaining attempts.
type Handler func(ctx context.Context, t *pb.Task) ([]byte, error)

type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error { return &PermanentError{Err: err} }

type Config struct {
	NodeID       string
	Queues       []string
	Concurrency  int
	LeaseTTL     time.Duration
	PollInterval time.Duration
	Handlers     map[string]Handler
	API          API
	Logger       *slog.Logger
	Metrics      *Metrics
}

func (c *Config) setDefaults() {
	if c.Concurrency == 0 {
		c.Concurrency = 4
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 15 * time.Second
	}
	if c.PollInterval == 0 {
		c.PollInterval = 200 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type Pool struct {
	cfg Config
	rng *backoff.Rand

	stopping chan struct{} // closed: don't lease anything new
	runCtx   context.Context
	cancel   context.CancelFunc // aborts running handlers
	dead     atomic.Bool        // simulated crash: touch nothing
	wg       sync.WaitGroup
	once     sync.Once
}

func New(cfg Config) *Pool {
	cfg.setDefaults()
	p := &Pool{cfg: cfg, rng: backoff.NewRand(time.Now().UnixNano()), stopping: make(chan struct{})}
	p.runCtx, p.cancel = context.WithCancel(context.Background())
	return p
}

func (p *Pool) Start() {
	for i := 0; i < p.cfg.Concurrency; i++ {
		p.wg.Add(1)
		go p.loop(fmt.Sprintf("%s/%d", p.cfg.NodeID, i))
	}
}

// Shutdown stops taking work and waits for running tasks. If ctx expires
// first, the stragglers are cancelled and handed back to the queue.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.once.Do(func() { close(p.stopping) })
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}
	p.cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
	}
	return ctx.Err()
}

// Kill simulates the process dying: no more calls to the cluster, running
// handlers are cancelled, nothing is released. Only meant for tests.
func (p *Pool) Kill() {
	p.dead.Store(true)
	p.once.Do(func() { close(p.stopping) })
	p.cancel()
}

func (p *Pool) isStopping() bool {
	select {
	case <-p.stopping:
		return true
	default:
		return false
	}
}

func (p *Pool) loop(workerID string) {
	defer p.wg.Done()
	idle := 0
	for !p.isStopping() {
		tasks, err := p.lease(workerID)
		if err != nil || len(tasks) == 0 {
			if err != nil {
				idle++
			} else {
				idle = 0
			}
			p.sleep(p.pollDelay(idle))
			continue
		}
		idle = 0
		for _, t := range tasks {
			if p.isStopping() {
				// we got it just as shutdown began; give it straight back
				p.release(t, "worker shutting down")
				continue
			}
			p.run(workerID, t)
		}
	}
}

func (p *Pool) lease(workerID string) ([]*pb.Task, error) {
	if p.dead.Load() {
		return nil, errors.New("dead")
	}
	// not tied to shutdown: if the leader already granted a lease, we want
	// the response so we can release it
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := p.cfg.API.Lease(ctx, &pb.LeaseRequest{
		WorkerId: workerID, Queues: p.cfg.Queues, MaxTasks: 1,
		LeaseTtlMs: uint32(p.cfg.LeaseTTL / time.Millisecond),
	})
	if err != nil {
		return nil, err
	}
	return resp.Tasks, nil
}

// poll interval with jitter, backing off while the cluster is unreachable
func (p *Pool) pollDelay(failures int) time.Duration {
	base := p.cfg.PollInterval
	if failures > 0 {
		pol := backoff.Policy{Base: base, Max: 5 * time.Second}
		return base/2 + pol.Delay(failures, p.rng)
	}
	return base/2 + time.Duration(p.rng.Int63n(int64(base)))
}

func (p *Pool) sleep(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-p.stopping:
	}
}

func (p *Pool) run(workerID string, t *pb.Task) {
	log := p.cfg.Logger.With("task", t.Id, "type", t.Type, "attempt", t.Attempt, "worker", workerID)
	h := p.cfg.Handlers[t.Type]
	if h == nil {
		log.Error("no handler registered, failing permanently")
		p.report(t, func(ctx context.Context) error {
			_, err := p.cfg.API.Fail(ctx, &pb.FailRequest{TaskId: t.Id, Token: t.LeaseToken, Error: "no handler for type " + t.Type, Permanent: true})
			return err
		})
		return
	}

	taskCtx, cancel := context.WithCancel(p.runCtx)
	defer cancel()
	var leaseLost atomic.Bool
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		p.heartbeat(taskCtx, t, &leaseLost, cancel, log)
	}()

	p.cfg.Metrics.started()
	start := time.Now()
	out, err := p.invoke(taskCtx, h, t)
	cancel()
	<-hbDone
	p.cfg.Metrics.finished(t.Type, err, time.Since(start))

	if p.dead.Load() {
		return
	}
	if leaseLost.Load() {
		log.Warn("lease lost while running, discarding result")
		return
	}

	switch {
	case err == nil:
		p.report(t, func(ctx context.Context) error {
			_, err := p.cfg.API.Complete(ctx, &pb.CompleteRequest{TaskId: t.Id, Token: t.LeaseToken, Result: out})
			return err
		})
	case p.runCtx.Err() != nil && errors.Is(err, context.Canceled):
		// shutdown deadline hit while it was running: not the task's fault
		p.release(t, "shutdown deadline")
	default:
		var perm *PermanentError
		isPerm := errors.As(err, &perm)
		log.Warn("task failed", "err", err, "permanent", isPerm)
		p.report(t, func(ctx context.Context) error {
			_, err2 := p.cfg.API.Fail(ctx, &pb.FailRequest{TaskId: t.Id, Token: t.LeaseToken, Error: truncate(err.Error(), 2048), Permanent: isPerm})
			return err2
		})
	}
}

func (p *Pool) invoke(ctx context.Context, h Handler, t *pb.Task) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, t)
}

// heartbeat extends the lease at a third of its TTL. Transient errors are
// ignored (the next tick tries again, and the deadline is the backstop); a
// fencing error means someone else owns the task now.
func (p *Pool) heartbeat(ctx context.Context, t *pb.Task, lost *atomic.Bool, cancelTask context.CancelFunc, log *slog.Logger) {
	tick := time.NewTicker(p.cfg.LeaseTTL / 3)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if p.dead.Load() {
			return
		}
		cctx, cancel := context.WithTimeout(ctx, p.cfg.LeaseTTL/3)
		_, err := p.cfg.API.ExtendLease(cctx, &pb.ExtendLeaseRequest{
			TaskId: t.Id, Token: t.LeaseToken, LeaseTtlMs: uint32(p.cfg.LeaseTTL / time.Millisecond),
		})
		cancel()
		if err != nil && apierr.Permanent(err) {
			lost.Store(true)
			cancelTask()
			return
		}
		if err != nil && ctx.Err() == nil {
			log.Debug("lease extension failed, will retry", "err", err)
		}
	}
}

// report delivers an outcome, retrying through leader changes. A fenced
// answer ends the retries: the task belongs to someone else now.
func (p *Pool) report(t *pb.Task, call func(ctx context.Context) error) {
	if p.dead.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := backoff.Retry(ctx, backoff.Policy{Base: 50 * time.Millisecond, Max: time.Second}, p.rng, 0, func(ctx context.Context) error {
		if p.dead.Load() {
			return &backoff.Permanent{Err: errors.New("dead")}
		}
		cctx, c := context.WithTimeout(ctx, 5*time.Second)
		defer c()
		if err := call(cctx); err != nil {
			if apierr.Permanent(err) {
				return &backoff.Permanent{Err: err}
			}
			return err
		}
		return nil
	})
	if err != nil {
		p.cfg.Logger.Warn("could not report task outcome", "task", t.Id, "err", err)
	}
}

func (p *Pool) release(t *pb.Task, why string) {
	p.cfg.Logger.Info("releasing task", "task", t.Id, "why", why)
	p.cfg.Metrics.released()
	p.report(t, func(ctx context.Context) error {
		_, err := p.cfg.API.Release(ctx, &pb.ReleaseRequest{TaskId: t.Id, Token: t.LeaseToken})
		return err
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
