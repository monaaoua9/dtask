package node_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/fsm"
	"github.com/monaaoua9/dtask/internal/linearize"
	"github.com/monaaoua9/dtask/internal/node"
	"github.com/monaaoua9/dtask/internal/raft"
)

// Linearizability under chaos. Clients lease, complete, release and read the
// same tasks through whichever node answers while nodes crash and links drop;
// every call is timestamped. Then, per task, the checker searches for a
// sequential order that respects real time and the lease/fencing rules.
//
// Calls that die with a timeout or a leadership change are "indefinite": they
// may or may not have happened, and the model has to be happy either way.

type taskState struct {
	phase uint8 // 0 pending, 1 leased, 2 succeeded
	tok   uint64
	wild  bool // leased under a token nobody has seen yet
	last  uint64
}

type linIn struct {
	kind string
	tok  uint64
}

type linOut struct {
	fenced bool
	phase  uint8
	tok    uint64
}

var taskModel = linearize.Model{
	Init: func() any { return taskState{} },
	Step: stepTask,
}

func stepTask(s any, op linearize.Op) (any, bool) {
	st, in := s.(taskState), op.In.(linIn)
	out, _ := op.Out.(linOut)

	// would a call with this token be accepted right now?
	holds := st.phase == 1 && ((!st.wild && st.tok == in.tok) || (st.wild && in.tok > st.last))
	already := st.phase == 2 && st.tok == in.tok
	bump := func(t uint64) uint64 {
		if t > st.last {
			return t
		}
		return st.last
	}

	switch in.kind {
	case "lease":
		if st.phase != 0 {
			return nil, false
		}
		if op.Indef {
			return taskState{phase: 1, wild: true, last: st.last}, true
		}
		if out.tok <= st.last {
			return nil, false
		}
		return taskState{phase: 1, tok: out.tok, last: out.tok}, true

	case "complete":
		if op.Indef || !out.fenced {
			if holds {
				return taskState{phase: 2, tok: in.tok, last: bump(in.tok)}, true
			}
			return st, already
		}
		return st, !holds && !already

	case "release":
		if op.Indef || !out.fenced {
			if holds {
				return taskState{phase: 0, last: bump(in.tok)}, true
			}
			return nil, false
		}
		return st, !holds

	case "get":
		if out.phase != st.phase {
			return nil, false
		}
		if st.phase != 1 {
			return st, true
		}
		if st.wild {
			if out.tok <= st.last {
				return nil, false
			}
			return taskState{phase: 1, tok: out.tok, last: out.tok}, true
		}
		return st, out.tok == st.tok
	}
	panic("unknown op " + in.kind)
}

type outcome int

const (
	done outcome = iota
	fenced
	unknown
	dropped // every node said "not leader" until we gave up; nothing happened
)

type linRun struct {
	c     *testCluster
	t     *testing.T
	start time.Time

	mu       sync.Mutex
	hist     map[string][]linearize.Op
	anyLease []linearize.Op // leases whose task we never learned
	stop     atomic.Bool
}

func (r *linRun) now() int64 { return int64(time.Since(r.start)) }

func (r *linRun) record(id string, op linearize.Op) {
	r.mu.Lock()
	r.hist[id] = append(r.hist[id], op)
	if len(r.hist[id]) >= 40 {
		r.stop.Store(true) // keep per-task histories small enough to check
	}
	r.mu.Unlock()
}

func noEffect(err error) bool {
	return errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrTransferring)
}

// one logical call: hop between nodes while they refuse with "not leader", but
// never resend after an error that might have been applied
func (r *linRun) call(rng *rand.Rand, fn func(ctx context.Context, n *node.Node) error) (outcome, error) {
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		nodes := r.c.alive()
		rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		for _, n := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			err := fn(ctx, n)
			cancel()
			switch {
			case err == nil:
				return done, nil
			case errors.Is(err, fsm.ErrFenced):
				return fenced, err
			case noEffect(err):
				continue
			default:
				return unknown, err
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return dropped, nil
}

func (r *linRun) client(id int, seed int64, ids []string) {
	rng := rand.New(rand.NewSource(seed))
	worker := fmt.Sprintf("lin-%d", id)
	held := map[string]uint64{}  // task -> token we believe we hold
	stale := map[string]uint64{} // tokens we gave up, to be replayed against fencing

	for !r.stop.Load() {
		time.Sleep(time.Duration(15+rng.Intn(35)) * time.Millisecond)

		switch p := rng.Intn(100); {
		case p < 30:
			r.mu.Lock()
			tooMany := len(r.anyLease) >= 10
			r.mu.Unlock()
			if tooMany {
				continue
			}
			var got *pb.Task
			call := r.now()
			res, _ := r.call(rng, func(ctx context.Context, n *node.Node) error {
				resp, err := n.Service().Lease(ctx, &pb.LeaseRequest{
					WorkerId: worker, Queues: []string{"lin"}, MaxTasks: 1, LeaseTtlMs: 600000,
				})
				if err == nil && len(resp.Tasks) > 0 {
					got = resp.Tasks[0]
				}
				return err
			})
			switch {
			case res == done && got != nil:
				r.record(got.Id, linearize.Op{Call: call, Ret: r.now(), In: linIn{kind: "lease"}, Out: linOut{tok: got.LeaseToken}})
				held[got.Id] = got.LeaseToken
			case res == unknown:
				r.mu.Lock()
				r.anyLease = append(r.anyLease, linearize.Op{Call: call, Indef: true, In: linIn{kind: "lease"}})
				r.mu.Unlock()
			}

		case p < 70:
			kind := "complete"
			if rng.Intn(3) == 0 {
				kind = "release"
			}
			id, tok, ok := pickToken(rng, held, stale)
			if !ok {
				continue
			}
			call := r.now()
			res, _ := r.call(rng, func(ctx context.Context, n *node.Node) (err error) {
				if kind == "complete" {
					_, err = n.Service().Complete(ctx, &pb.CompleteRequest{TaskId: id, Token: tok, Result: []byte("r")})
				} else {
					_, err = n.Service().Release(ctx, &pb.ReleaseRequest{TaskId: id, Token: tok})
				}
				return
			})
			if res == dropped {
				continue
			}
			op := linearize.Op{Call: call, Ret: r.now(), Indef: res == unknown, In: linIn{kind: kind, tok: tok}, Out: linOut{fenced: res == fenced}}
			r.record(id, op)
			if held[id] == tok {
				delete(held, id)
			}
			stale[id] = tok

		default:
			id := ids[rng.Intn(len(ids))]
			var got *pb.Task
			call := r.now()
			res, _ := r.call(rng, func(ctx context.Context, n *node.Node) error {
				resp, err := n.Service().GetTask(ctx, &pb.GetTaskRequest{Id: id})
				if err == nil {
					got = resp.Task
				}
				return err
			})
			if res != done { // a read that never answered tells us nothing
				continue
			}
			out := linOut{tok: got.LeaseToken}
			switch got.State {
			case pb.TaskState_TASK_STATE_PENDING:
				out.phase = 0
			case pb.TaskState_TASK_STATE_LEASED:
				out.phase = 1
			case pb.TaskState_TASK_STATE_SUCCEEDED:
				out.phase = 2
			default:
				r.t.Errorf("task %s in unexpected state %v", id, got.State)
				return
			}
			r.record(id, linearize.Op{Call: call, Ret: r.now(), In: linIn{kind: "get"}, Out: out})
		}
	}
}

// mostly a token we really hold, sometimes one we already gave up (must be fenced)
func pickToken(rng *rand.Rand, held, stale map[string]uint64) (string, uint64, bool) {
	pool := held
	if len(held) == 0 || rng.Intn(5) == 0 {
		pool = stale
	}
	n := rng.Intn(len(pool) + 1)
	for id, tok := range pool {
		if n == 0 {
			return id, tok, true
		}
		n--
	}
	return "", 0, false
}

func TestLinearizableUnderChaos(t *testing.T) {
	seeds := []int64{101, 202, 303}
	if testing.Short() {
		seeds = seeds[:1]
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runLinearizable(t, seed) })
	}
}

func runLinearizable(t *testing.T, seed int64) {
	c := newTestCluster(t, 5, func(cfg *node.Config) { cfg.SnapshotThreshold = 60 })
	c.leader()

	const tasks = 60
	ids := make([]string, tasks)
	for i := range ids {
		ids[i] = c.submit(fmt.Sprint("lin-", i), workSpec{}, func(r *pb.SubmitRequest) { r.Queue = "lin" }).Id
	}

	r := &linRun{c: c, t: t, start: time.Now(), hist: map[string][]linearize.Op{}}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.client(i, seed*10+int64(i), ids)
		}(i)
	}

	rng := rand.New(rand.NewSource(seed))
	down := map[raft.NodeID]bool{}
	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) && !r.stop.Load() {
		time.Sleep(time.Duration(200+rng.Intn(300)) * time.Millisecond)
		id := c.ids[rng.Intn(len(c.ids))]
		switch rng.Intn(6) {
		case 0:
			if len(down) < 2 && !down[id] {
				c.crash(id)
				down[id] = true
			}
		case 1:
			for d := range down {
				c.restart(d)
				delete(down, d)
				break
			}
		case 2:
			if !down[id] {
				c.net.Heal()
				c.isolate(id)
			}
		case 3:
			c.net.Heal()
			c.net.SetDropRate(0)
			c.net.SetLatency(0, 0)
		case 4:
			c.net.SetDropRate(0.1)
		case 5:
			c.net.SetLatency(0, 15*time.Millisecond)
		}
	}
	r.stop.Store(true)
	wg.Wait()

	c.net.Heal()
	c.net.SetDropRate(0)
	c.net.SetLatency(0, 0)
	for d := range down {
		c.restart(d)
	}

	total, withUnknown := 0, len(r.anyLease)
	for _, id := range ids {
		ops := append(append([]linearize.Op(nil), r.hist[id]...), r.anyLease...)
		total += len(r.hist[id])
		require.LessOrEqual(t, len(ops), linearize.MaxOps)
		if !linearize.Check(ops, taskModel) {
			t.Fatalf("task %s: history is not linearizable\n%s", id, linearize.Describe(ops))
		}
	}
	t.Logf("seed=%d: %d operations over %d tasks checked, %d leases of unknown outcome", seed, total, tasks, withUnknown)
	require.Greater(t, total, 200, "too few operations got through to mean anything")
}
