// Package fsm is the replicated task state machine. It is a pure function of
// the command log: no clocks, no randomness, no map-order dependence in
// anything observable. Every replica that applies the same commands ends up
// byte-for-byte identical (see Digest).
package fsm

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
)

type Result struct{}

type SubmitResult struct {
	Task      *pb.Task
	Duplicate bool
}

type LeaseResult struct{ Tasks []*pb.Task }

type ExtendResult struct{ ExpiresAtNs int64 }

type TaskResult struct{ Task *pb.Task }

type ReapResult struct{ Expired, Collected int }

type ExpireNodeResult struct{ Released int }

type FSM struct {
	mu     sync.RWMutex
	tasks  map[string]*pb.Task
	idem   map[string]string // queue\x00key -> task id
	queues map[string]*queue
	qver   map[string]uint64
	leased map[string]*pb.Task

	ver       uint64 // heap entry versions, not replicated state
	nextSeq   uint64
	nextToken uint64
	clock     int64
}

func New() *FSM {
	f := &FSM{}
	f.reset()
	return f
}

func (f *FSM) reset() {
	f.tasks = map[string]*pb.Task{}
	f.idem = map[string]string{}
	f.queues = map[string]*queue{}
	f.qver = map[string]uint64{}
	f.leased = map[string]*pb.Task{}
	f.ver, f.nextSeq, f.nextToken, f.clock = 0, 1, 1, 0
}

func (f *FSM) queue(name string) *queue {
	q := f.queues[name]
	if q == nil {
		q = newQueue(name)
		f.queues[name] = q
	}
	return q
}

func (f *FSM) advance(now int64) int64 {
	if now > f.clock {
		f.clock = now
	}
	return f.clock
}

func idemKey(queue, key string) string { return queue + "\x00" + key }

// Apply implements raft.FSM. Failures of individual commands (fenced token,
// unknown task, ...) are returned as error values, they never panic.
func (f *FSM) Apply(_ uint64, data []byte) any {
	var cmd pb.Command
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return fmt.Errorf("%w: %v", ErrBadCommand, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	switch c := cmd.Cmd.(type) {
	case *pb.Command_Submit:
		return f.applySubmit(c.Submit)
	case *pb.Command_Lease:
		return f.applyLease(c.Lease)
	case *pb.Command_Extend:
		return f.applyExtend(c.Extend)
	case *pb.Command_Complete:
		return f.applyComplete(c.Complete)
	case *pb.Command_Fail:
		return f.applyFail(c.Fail)
	case *pb.Command_Release:
		return f.applyRelease(c.Release)
	case *pb.Command_Reap:
		return f.applyReap(c.Reap)
	case *pb.Command_ExpireNode:
		return f.applyExpireNode(c.ExpireNode)
	case *pb.Command_SetQueueConfig:
		return f.applySetQueueConfig(c.SetQueueConfig)
	case *pb.Command_Cancel:
		return f.applyCancel(c.Cancel)
	}
	return ErrBadCommand
}

func (f *FSM) applySubmit(c *pb.SubmitCmd) any {
	now := f.advance(c.NowNs)
	if c.IdempotencyKey != "" {
		if id, ok := f.idem[idemKey(c.Queue, c.IdempotencyKey)]; ok {
			return &SubmitResult{Task: proto.Clone(f.tasks[id]).(*pb.Task), Duplicate: true}
		}
	}
	if _, dup := f.tasks[c.Id]; dup {
		return ErrExists
	}
	retry := c.Retry
	if retry == nil || retry.MaxAttempts == 0 {
		retry = &pb.RetryPolicy{MaxAttempts: 1, BaseDelayMs: retry.GetBaseDelayMs(), MaxDelayMs: retry.GetMaxDelayMs()}
	}
	avail := c.AvailableAtNs
	if avail < now {
		avail = now
	}
	t := &pb.Task{
		Id: c.Id, Queue: c.Queue, Type: c.Type, Payload: c.Payload, Priority: c.Priority,
		State: pb.TaskState_TASK_STATE_PENDING, Retry: retry, IdempotencyKey: c.IdempotencyKey,
		CreatedAtNs: now, AvailableAtNs: avail, Seq: f.nextSeq,
	}
	f.nextSeq++
	f.tasks[t.Id] = t
	if t.IdempotencyKey != "" {
		f.idem[idemKey(t.Queue, t.IdempotencyKey)] = t.Id
	}
	f.enqueue(t, now)
	return &SubmitResult{Task: proto.Clone(t).(*pb.Task)}
}

func (f *FSM) applyLease(c *pb.LeaseCmd) any {
	now := f.advance(c.NowNs)
	max := int(c.MaxTasks)
	if max < 1 {
		max = 1
	}
	var out []*pb.Task
	for _, name := range c.Queues {
		q := f.queues[name]
		if q == nil {
			continue
		}
		f.promote(q, now)
		q.refill(now)
		for len(out) < max {
			if q.cfg.MaxInFlight > 0 && q.inFlight >= int(q.cfg.MaxInFlight) {
				break
			}
			if q.limited() && q.tokens < 1000 {
				break
			}
			t := f.popReady(q)
			if t == nil {
				break
			}
			if q.limited() {
				q.tokens -= 1000
			}
			t.State = pb.TaskState_TASK_STATE_LEASED
			t.Attempt++
			t.LeaseWorker = c.WorkerId
			t.LeaseToken = f.nextToken
			f.nextToken++
			t.LeaseExpiresAtNs = now + c.TtlNs
			q.inFlight++
			f.leased[t.Id] = t
			out = append(out, proto.Clone(t).(*pb.Task))
		}
	}
	return &LeaseResult{Tasks: out}
}

// holder checks the fencing token. A stale token means the lease was lost
// (expired, node declared dead, cancelled) and somebody else may own the task.
func (f *FSM) holder(id string, token uint64) (*pb.Task, error) {
	t := f.tasks[id]
	if t == nil {
		return nil, ErrNotFound
	}
	if t.State != pb.TaskState_TASK_STATE_LEASED || t.LeaseToken != token {
		return t, ErrFenced
	}
	return t, nil
}

func (f *FSM) applyExtend(c *pb.ExtendCmd) any {
	now := f.advance(c.NowNs)
	t, err := f.holder(c.TaskId, c.Token)
	if err != nil {
		return err
	}
	t.LeaseExpiresAtNs = now + c.TtlNs
	return &ExtendResult{ExpiresAtNs: t.LeaseExpiresAtNs}
}

func (f *FSM) unlease(t *pb.Task) {
	delete(f.leased, t.Id)
	if q := f.queues[t.Queue]; q != nil && q.inFlight > 0 {
		q.inFlight--
	}
	t.LeaseExpiresAtNs = 0
}

func (f *FSM) applyComplete(c *pb.CompleteCmd) any {
	now := f.advance(c.NowNs)
	t, err := f.holder(c.TaskId, c.Token)
	if err != nil {
		// a retried Complete after a lost response must not look like a failure
		if t != nil && t.State == pb.TaskState_TASK_STATE_SUCCEEDED && t.LeaseToken == c.Token {
			return &Result{}
		}
		return err
	}
	f.unlease(t)
	t.State = pb.TaskState_TASK_STATE_SUCCEEDED
	t.FinishedAtNs = now
	t.Result = c.Result
	t.LastError = ""
	return &Result{}
}

func (f *FSM) applyFail(c *pb.FailCmd) any {
	now := f.advance(c.NowNs)
	t, err := f.holder(c.TaskId, c.Token)
	if err != nil {
		return err
	}
	f.unlease(t)
	t.LastError = c.Error
	f.retryOrBury(t, now, c.Permanent, c.RetryAtNs)
	return &TaskResult{Task: proto.Clone(t).(*pb.Task)}
}

func (f *FSM) retryOrBury(t *pb.Task, now int64, permanent bool, retryAt int64) {
	if permanent || t.Attempt >= t.Retry.MaxAttempts {
		t.State = pb.TaskState_TASK_STATE_DEAD
		t.FinishedAtNs = now
		return
	}
	t.State = pb.TaskState_TASK_STATE_PENDING
	if retryAt < now {
		retryAt = now
	}
	t.AvailableAtNs = retryAt
	f.enqueue(t, now)
}

func (f *FSM) applyRelease(c *pb.ReleaseCmd) any {
	now := f.advance(c.NowNs)
	t, err := f.holder(c.TaskId, c.Token)
	if err != nil {
		return err
	}
	f.unlease(t)
	// handed back untouched (graceful shutdown), so it doesn't cost an attempt
	if t.Attempt > 0 {
		t.Attempt--
	}
	t.State = pb.TaskState_TASK_STATE_PENDING
	t.AvailableAtNs = now
	f.enqueue(t, now)
	return &Result{}
}

func (f *FSM) applyCancel(c *pb.CancelCmd) any {
	now := f.advance(c.NowNs)
	t := f.tasks[c.TaskId]
	if t == nil {
		return ErrNotFound
	}
	switch t.State {
	case pb.TaskState_TASK_STATE_PENDING:
		delete(f.qver, t.Id)
	case pb.TaskState_TASK_STATE_LEASED:
		f.unlease(t)
	default:
		return ErrWrongState
	}
	t.State = pb.TaskState_TASK_STATE_CANCELED
	t.FinishedAtNs = now
	return &TaskResult{Task: proto.Clone(t).(*pb.Task)}
}

func (f *FSM) expireLease(t *pb.Task, now int64, reason string) {
	f.unlease(t)
	t.LastError = reason
	f.retryOrBury(t, now, false, now)
}

func (f *FSM) applyReap(c *pb.ReapCmd) any {
	now := f.advance(c.NowNs)
	var res ReapResult
	for _, t := range f.leased {
		if t.LeaseExpiresAtNs <= now {
			f.expireLease(t, now, "lease expired")
			res.Expired++
		}
	}
	if c.RetentionNs > 0 {
		for id, t := range f.tasks {
			if t.FinishedAtNs == 0 || t.FinishedAtNs+c.RetentionNs > now {
				continue
			}
			delete(f.tasks, id)
			delete(f.qver, id)
			if t.IdempotencyKey != "" {
				delete(f.idem, idemKey(t.Queue, t.IdempotencyKey))
			}
			res.Collected++
		}
	}
	return &res
}

// applyExpireNode is what makes failover fast: once the leader's failure
// detector gives up on a node, everything its workers hold goes straight back
// into the queue instead of waiting for each lease to run out.
func (f *FSM) applyExpireNode(c *pb.ExpireNodeCmd) any {
	now := f.advance(c.NowNs)
	prefix := c.NodeId + "/"
	var res ExpireNodeResult
	for _, t := range f.leased {
		if strings.HasPrefix(t.LeaseWorker, prefix) {
			f.expireLease(t, now, "node "+c.NodeId+" declared dead")
			res.Released++
		}
	}
	return &res
}

func (f *FSM) applySetQueueConfig(c *pb.SetQueueConfigCmd) any {
	now := f.advance(c.NowNs)
	cfg := proto.Clone(c.Config).(*pb.QueueConfig)
	f.queue(cfg.Name).configure(cfg, now)
	return &Result{}
}

// Snapshot serialises the whole state deterministically.
func (f *FSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	snap := &pb.FSMSnapshot{NextSeq: f.nextSeq, NextToken: f.nextToken, ClockNs: f.clock}
	for _, t := range f.tasks {
		snap.Tasks = append(snap.Tasks, t)
	}
	sort.Slice(snap.Tasks, func(i, j int) bool { return snap.Tasks[i].Seq < snap.Tasks[j].Seq })
	names := make([]string, 0, len(f.queues))
	for n := range f.queues {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		q := f.queues[n]
		snap.Queues = append(snap.Queues, &pb.QueueState{Config: q.cfg, TokensMilli: q.tokens, LastRefillNs: q.lastRefill})
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(snap)
}

func (f *FSM) Restore(data []byte) error {
	var snap pb.FSMSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("fsm restore: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reset()
	f.nextSeq, f.nextToken, f.clock = snap.NextSeq, snap.NextToken, snap.ClockNs
	for _, qs := range snap.Queues {
		q := newQueue(qs.Config.Name)
		q.cfg, q.tokens, q.lastRefill = qs.Config, qs.TokensMilli, qs.LastRefillNs
		f.queues[qs.Config.Name] = q
	}
	for _, t := range snap.Tasks { // already in seq order
		f.tasks[t.Id] = t
		if t.IdempotencyKey != "" {
			f.idem[idemKey(t.Queue, t.IdempotencyKey)] = t.Id
		}
		switch t.State {
		case pb.TaskState_TASK_STATE_PENDING:
			f.enqueue(t, f.clock)
		case pb.TaskState_TASK_STATE_LEASED:
			f.leased[t.Id] = t
			f.queue(t.Queue).inFlight++
		}
	}
	return nil
}

// Digest fingerprints the state, for comparing replicas in tests.
func (f *FSM) Digest() string {
	b, err := f.Snapshot()
	if err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
