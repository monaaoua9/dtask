package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/apierr"
	"github.com/monaaoua9/dtask/internal/node"
	"github.com/monaaoua9/dtask/internal/raft"
	"github.com/monaaoua9/dtask/internal/raft/simnet"
	"github.com/monaaoua9/dtask/internal/worker"
)

// what a "work" task payload tells the handler to do
type workSpec struct {
	Ms        int `json:"ms"`
	FailFirst int `json:"fail_first"` // fail this many attempts before succeeding
	AlwaysErr bool
}

type execution struct {
	TaskID  string
	Token   uint64
	Attempt uint32
	Node    raft.NodeID
	Start   time.Time
	End     time.Time
	Err     error
}

type recorder struct {
	mu    sync.Mutex
	execs []execution
}

func (r *recorder) begin(e execution) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.execs = append(r.execs, e)
	return len(r.execs) - 1
}

func (r *recorder) end(i int, err error) {
	r.mu.Lock()
	r.execs[i].End, r.execs[i].Err = time.Now(), err
	r.mu.Unlock()
}

func (r *recorder) startedOn(id raft.NodeID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.execs {
		if e.Node == id {
			return true
		}
	}
	return false
}

func (r *recorder) all() []execution {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]execution(nil), r.execs...)
}

func (r *recorder) byTask() map[string][]execution {
	out := map[string][]execution{}
	for _, e := range r.all() {
		out[e.TaskID] = append(out[e.TaskID], e)
	}
	return out
}

type testCluster struct {
	t    *testing.T
	net  *simnet.Network
	rec  *recorder
	mu   sync.Mutex
	live map[raft.NodeID]*node.Node
	cfg  map[raft.NodeID]node.Config
	ids  []raft.NodeID
}

type tcOpt func(*node.Config)

func newTestCluster(t *testing.T, n int, opts ...tcOpt) *testCluster {
	t.Helper()
	c := &testCluster{
		t: t, net: simnet.New(time.Now().UnixNano()), rec: &recorder{},
		live: map[raft.NodeID]*node.Node{}, cfg: map[raft.NodeID]node.Config{},
	}
	var boot []raft.Server
	for i := 1; i <= n; i++ {
		id := raft.NodeID(fmt.Sprintf("n%d", i))
		c.ids = append(c.ids, id)
		boot = append(boot, raft.Server{ID: id, Addr: string(id), Voter: true})
	}
	for _, id := range c.ids {
		cfg := node.Config{
			ID: id, AdvertiseAddr: string(id), DataDir: t.TempDir(), NoFsync: true,
			Bootstrap: boot, Queues: []string{"default"},
			Concurrency: 3, LeaseTTL: 30 * time.Second, PollInterval: 15 * time.Millisecond,
			Heartbeat: 15 * time.Millisecond, ElectionMin: 150 * time.Millisecond, ElectionMax: 300 * time.Millisecond,
			SnapshotThreshold: 200,
			Transport:         c.net.Endpoint(id),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		cfg.Scheduler.Tick = 20 * time.Millisecond
		cfg.Handlers = map[string]worker.Handler{"work": c.workHandler(id)}
		for _, o := range opts {
			o(&cfg)
		}
		c.cfg[id] = cfg
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		var all []*node.Node
		for _, n := range c.live {
			all = append(all, n)
		}
		c.mu.Unlock()
		for _, n := range all {
			n.Crash()
		}
	})
	return c
}

func (c *testCluster) workHandler(id raft.NodeID) worker.Handler {
	return func(ctx context.Context, t *pb.Task) ([]byte, error) {
		var spec workSpec
		_ = json.Unmarshal(t.Payload, &spec)
		idx := c.rec.begin(execution{TaskID: t.Id, Token: t.LeaseToken, Attempt: t.Attempt, Node: id, Start: time.Now()})

		select {
		case <-time.After(time.Duration(spec.Ms) * time.Millisecond):
		case <-ctx.Done():
			c.rec.end(idx, ctx.Err())
			return nil, ctx.Err()
		}
		if spec.AlwaysErr || int(t.Attempt) <= spec.FailFirst {
			err := fmt.Errorf("simulated failure on attempt %d", t.Attempt)
			c.rec.end(idx, err)
			return nil, err
		}
		c.rec.end(idx, nil)
		return []byte("done:" + string(id)), nil
	}
}

func (c *testCluster) start(id raft.NodeID) *node.Node {
	cfg := c.cfg[id]
	cfg.WorkerAPI = &router{c: c, from: id}
	n, err := node.New(cfg)
	require.NoError(c.t, err)
	c.net.Register(id, n.Raft())
	c.mu.Lock()
	c.live[id] = n
	c.mu.Unlock()
	n.Start()
	return n
}

func (c *testCluster) dump() string {
	out := ""
	for _, n := range c.alive() {
		out += "\n  " + n.Raft().Debug()
	}
	return out + fmt.Sprintf(" blocked=%v", c.net.Blocked())
}

func (c *testCluster) addMember(id raft.NodeID, cfg node.Config) {
	c.mu.Lock()
	c.cfg[id] = cfg
	c.ids = append(c.ids, id)
	c.mu.Unlock()
}

func (c *testCluster) get(id raft.NodeID) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.live[id]
}

func (c *testCluster) alive() []*node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*node.Node
	for _, id := range c.ids {
		if n := c.live[id]; n != nil {
			out = append(out, n)
		}
	}
	return out
}

func (c *testCluster) crash(id raft.NodeID) {
	c.mu.Lock()
	n := c.live[id]
	delete(c.live, id)
	c.mu.Unlock()
	if n == nil {
		return
	}
	c.net.Unregister(id)
	n.Crash()
}

func (c *testCluster) restart(id raft.NodeID) { c.start(id) }

func (c *testCluster) leader() *node.Node {
	c.t.Helper()
	var l *node.Node
	require.Eventually(c.t, func() bool {
		for _, n := range c.alive() {
			if n.Raft().IsLeader() {
				l = n
				return true
			}
		}
		return false
	}, 10*time.Second, 5*time.Millisecond, "no leader")
	return l
}

func (c *testCluster) isolate(id raft.NodeID) {
	var others []raft.NodeID
	for _, o := range c.ids {
		if o != id {
			others = append(others, o)
		}
	}
	c.net.Isolate(id, others)
}

// submit goes through whichever node is leader, retrying; the key makes that safe.
func (c *testCluster) submit(key string, spec workSpec, mut ...func(*pb.SubmitRequest)) *pb.Task {
	c.t.Helper()
	body, _ := json.Marshal(spec)
	req := &pb.SubmitRequest{Queue: "default", Type: "work", Payload: body, IdempotencyKey: key}
	for _, m := range mut {
		m(req)
	}
	r := &router{c: c, from: ""}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := r.Submit(ctx, req)
	require.NoError(c.t, err, c.dump())
	return resp.Task
}

// stats reads the state machine of whichever node currently leads.
func (c *testCluster) counts() (pending, delayed, leased, done, dead uint64) {
	for _, n := range c.alive() {
		if !n.Raft().IsLeader() {
			continue
		}
		for _, q := range n.FSM().Stats(time.Now().UnixNano()) {
			pending += q.Pending
			delayed += q.Delayed
			leased += q.Leased
			done += q.Succeeded
			dead += q.Dead
		}
		return
	}
	return
}

func (c *testCluster) waitFinished(total int, within time.Duration) {
	c.t.Helper()
	require.Eventually(c.t, func() bool {
		_, _, _, done, dead := c.counts()
		return int(done+dead) >= total
	}, within, 20*time.Millisecond, func() string {
		p, d, l, ok, dd := c.counts()
		return fmt.Sprintf("only pending=%d delayed=%d leased=%d done=%d dead=%d of %d", p, d, l, ok, dd, total)
	}())
}

// every live replica must hold exactly the same state
func (c *testCluster) assertConverged(within time.Duration) {
	c.t.Helper()
	require.Eventually(c.t, func() bool {
		var ref string
		for i, n := range c.alive() {
			d := n.FSM().Digest()
			if i == 0 {
				ref = d
			} else if d != ref {
				return false
			}
		}
		return true
	}, within, 20*time.Millisecond, "replicas diverged")
}

func (c *testCluster) allTasks() []*pb.Task {
	for _, n := range c.alive() {
		if n.Raft().IsLeader() {
			tasks, _ := n.FSM().List("", pb.TaskState_TASK_STATE_UNSPECIFIED, 1<<30, 0)
			return tasks
		}
	}
	return nil
}

func tokensUnique(t *testing.T, execs []execution) {
	t.Helper()
	seen := map[uint64]string{}
	for _, e := range execs {
		if prev, dup := seen[e.Token]; dup {
			t.Fatalf("lease token %d handed out twice (tasks %s and %s)", e.Token, prev, e.TaskID)
		}
		seen[e.Token] = e.TaskID
	}
}

// router is the in-process stand-in for the gRPC client: it finds a
// leader among the nodes this caller can actually reach on the fake network.
type router struct {
	c    *testCluster
	from raft.NodeID // "" means a client outside the cluster, never partitioned
}

func retryable(err error) bool {
	return errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) ||
		errors.Is(err, raft.ErrStopped) || errors.Is(err, raft.ErrTransferring) ||
		errors.Is(err, raft.ErrLeaderNotReady) || errors.Is(err, context.DeadlineExceeded)
}

func (r *router) try(ctx context.Context, fn func(ctx context.Context, n *node.Node) error) error {
	for {
		for _, n := range r.c.alive() {
			if r.from != "" && (!r.c.net.Reachable(r.from, n.ID()) || !r.c.net.Reachable(n.ID(), r.from)) {
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := fn(cctx, n)
			cancel()
			if err == nil || !retryable(err) {
				return err
			}
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return apierr.ErrUnavailable
		}
	}
}

func (r *router) Submit(ctx context.Context, req *pb.SubmitRequest) (out *pb.SubmitResponse, err error) {
	err = r.try(ctx, func(ctx context.Context, n *node.Node) (e error) {
		out, e = n.Service().Submit(ctx, req)
		return
	})
	return
}

func (r *router) Lease(ctx context.Context, req *pb.LeaseRequest) (out *pb.LeaseResponse, err error) {
	err = r.try(ctx, func(ctx context.Context, n *node.Node) (e error) {
		out, e = n.Service().Lease(ctx, req)
		return
	})
	return
}

func (r *router) ExtendLease(ctx context.Context, req *pb.ExtendLeaseRequest) (out *pb.ExtendLeaseResponse, err error) {
	err = r.try(ctx, func(ctx context.Context, n *node.Node) (e error) {
		out, e = n.Service().ExtendLease(ctx, req)
		return
	})
	return
}

func (r *router) Complete(ctx context.Context, req *pb.CompleteRequest) (out *pb.CompleteResponse, err error) {
	err = r.try(ctx, func(ctx context.Context, n *node.Node) (e error) {
		out, e = n.Service().Complete(ctx, req)
		return
	})
	return
}

func (r *router) Fail(ctx context.Context, req *pb.FailRequest) (out *pb.FailResponse, err error) {
	err = r.try(ctx, func(ctx context.Context, n *node.Node) (e error) {
		out, e = n.Service().Fail(ctx, req)
		return
	})
	return
}

func (r *router) Release(ctx context.Context, req *pb.ReleaseRequest) (out *pb.ReleaseResponse, err error) {
	err = r.try(ctx, func(ctx context.Context, n *node.Node) (e error) {
		out, e = n.Service().Release(ctx, req)
		return
	})
	return
}
