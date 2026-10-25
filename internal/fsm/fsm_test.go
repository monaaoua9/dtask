package fsm

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
)

const sec = int64(time.Second)

func enc(t *testing.T, c *pb.Command) []byte {
	t.Helper()
	b, err := proto.Marshal(c)
	require.NoError(t, err)
	return b
}

type driver struct {
	t   *testing.T
	f   *FSM
	idx uint64
}

func newDriver(t *testing.T) *driver { return &driver{t: t, f: New()} }

func (d *driver) do(c *pb.Command) any {
	d.idx++
	return d.f.Apply(d.idx, enc(d.t, c))
}

func (d *driver) submit(id, queue string, now int64, mut ...func(*pb.SubmitCmd)) any {
	c := &pb.SubmitCmd{Id: id, Queue: queue, Type: "t", NowNs: now, Retry: &pb.RetryPolicy{MaxAttempts: 3}}
	for _, m := range mut {
		m(c)
	}
	return d.do(&pb.Command{Cmd: &pb.Command_Submit{Submit: c}})
}

func (d *driver) lease(worker string, now int64, queues ...string) []*pb.Task {
	r := d.do(&pb.Command{Cmd: &pb.Command_Lease{Lease: &pb.LeaseCmd{
		WorkerId: worker, Queues: queues, MaxTasks: 10, TtlNs: 10 * sec, NowNs: now,
	}}})
	return r.(*LeaseResult).Tasks
}

func (d *driver) complete(id string, token uint64, now int64) any {
	return d.do(&pb.Command{Cmd: &pb.Command_Complete{Complete: &pb.CompleteCmd{TaskId: id, Token: token, NowNs: now}}})
}

func (d *driver) fail(id string, token uint64, now, retryAt int64, permanent bool) any {
	return d.do(&pb.Command{Cmd: &pb.Command_Fail{Fail: &pb.FailCmd{TaskId: id, Token: token, Error: "boom", NowNs: now, RetryAtNs: retryAt, Permanent: permanent}}})
}

func (d *driver) reap(now int64) *ReapResult {
	return d.do(&pb.Command{Cmd: &pb.Command_Reap{Reap: &pb.ReapCmd{NowNs: now}}}).(*ReapResult)
}

func (d *driver) state(id string) pb.TaskState {
	tk, ok := d.f.Get(id)
	require.True(d.t, ok)
	return tk.State
}

func TestSubmitLeaseComplete(t *testing.T) {
	d := newDriver(t)
	res := d.submit("a", "q", sec).(*SubmitResult)
	require.False(t, res.Duplicate)

	got := d.lease("n1/0", 2*sec, "q")
	require.Len(t, got, 1)
	require.Equal(t, uint32(1), got[0].Attempt)
	require.Equal(t, pb.TaskState_TASK_STATE_LEASED, got[0].State)
	require.Equal(t, 12*sec, got[0].LeaseExpiresAtNs)

	require.NoError(t, asErr(d.complete("a", got[0].LeaseToken, 3*sec)))
	require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, d.state("a"))
	require.Empty(t, d.lease("n1/0", 4*sec, "q"))
}

func asErr(v any) error {
	if e, ok := v.(error); ok {
		return e
	}
	return nil
}

func TestIdempotentSubmit(t *testing.T) {
	d := newDriver(t)
	mut := func(c *pb.SubmitCmd) { c.IdempotencyKey = "order-42" }
	first := d.submit("a", "q", sec, mut).(*SubmitResult)
	second := d.submit("b", "q", sec, mut).(*SubmitResult)
	require.True(t, second.Duplicate)
	require.Equal(t, first.Task.Id, second.Task.Id)
	require.Equal(t, 1, d.f.Len())

	// same key, other queue is a different task
	other := d.submit("c", "q2", sec, mut).(*SubmitResult)
	require.False(t, other.Duplicate)
}

func TestPriorityThenFIFO(t *testing.T) {
	d := newDriver(t)
	d.submit("low1", "q", sec)
	d.submit("low2", "q", sec)
	d.submit("hi", "q", sec, func(c *pb.SubmitCmd) { c.Priority = 5 })
	var order []string
	for _, tk := range d.lease("w", 2*sec, "q") {
		order = append(order, tk.Id)
	}
	require.Equal(t, []string{"hi", "low1", "low2"}, order)
}

func TestDelayedTaskWaitsForItsTime(t *testing.T) {
	d := newDriver(t)
	d.submit("later", "q", sec, func(c *pb.SubmitCmd) { c.AvailableAtNs = 10 * sec })
	require.Empty(t, d.lease("w", 5*sec, "q"))
	require.False(t, d.f.HasReady([]string{"q"}, 5*sec))
	require.True(t, d.f.HasReady([]string{"q"}, 10*sec))
	require.Len(t, d.lease("w", 10*sec, "q"), 1)
}

func TestFencingRejectsZombieWorker(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec)
	old := d.lease("slow", 2*sec, "q")[0]

	// slow worker stalls past its lease; the reaper hands the task to someone else
	require.Equal(t, 1, d.reap(20*sec).Expired)
	fresh := d.lease("fast", 21*sec, "q")[0]
	require.Greater(t, fresh.LeaseToken, old.LeaseToken)

	// the zombie wakes up and tries to report
	require.ErrorIs(t, asErr(d.complete("a", old.LeaseToken, 22*sec)), ErrFenced)
	require.Equal(t, pb.TaskState_TASK_STATE_LEASED, d.state("a"))

	require.NoError(t, asErr(d.complete("a", fresh.LeaseToken, 23*sec)))
	require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, d.state("a"))
}

func TestCompleteIsIdempotentForTheSameToken(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec)
	tk := d.lease("w", sec, "q")[0]
	require.NoError(t, asErr(d.complete("a", tk.LeaseToken, 2*sec)))
	require.NoError(t, asErr(d.complete("a", tk.LeaseToken, 3*sec)), "retry after a lost response")
	require.ErrorIs(t, asErr(d.complete("a", tk.LeaseToken+1, 3*sec)), ErrFenced)
}

func TestFailRetriesThenDies(t *testing.T) {
	d := newDriver(t) // 3 attempts
	d.submit("a", "q", sec)

	tk := d.lease("w", 2*sec, "q")[0]
	r := d.fail("a", tk.LeaseToken, 3*sec, 8*sec, false).(*TaskResult)
	require.Equal(t, pb.TaskState_TASK_STATE_PENDING, r.Task.State)
	require.Equal(t, 8*sec, r.Task.AvailableAtNs)
	require.Empty(t, d.lease("w", 5*sec, "q"), "backoff not over yet")

	tk = d.lease("w", 8*sec, "q")[0]
	require.Equal(t, uint32(2), tk.Attempt)
	d.fail("a", tk.LeaseToken, 9*sec, 9*sec, false)
	tk = d.lease("w", 9*sec, "q")[0]
	r = d.fail("a", tk.LeaseToken, 10*sec, 10*sec, false).(*TaskResult)
	require.Equal(t, pb.TaskState_TASK_STATE_DEAD, r.Task.State)
	require.Equal(t, "boom", r.Task.LastError)
}

func TestPermanentFailureSkipsRetries(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec)
	tk := d.lease("w", sec, "q")[0]
	r := d.fail("a", tk.LeaseToken, 2*sec, 0, true).(*TaskResult)
	require.Equal(t, pb.TaskState_TASK_STATE_DEAD, r.Task.State)
}

func TestReleaseDoesNotCostAnAttempt(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec)
	tk := d.lease("w", sec, "q")[0]
	require.NoError(t, asErr(d.do(&pb.Command{Cmd: &pb.Command_Release{Release: &pb.ReleaseCmd{TaskId: "a", Token: tk.LeaseToken, NowNs: 2 * sec}}})))
	again := d.lease("w2", 3*sec, "q")[0]
	require.Equal(t, uint32(1), again.Attempt)
}

func TestReapOnlyTouchesExpiredLeases(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec)
	d.submit("b", "q", sec)
	d.lease("w", 2*sec, "q") // both leased, expire at 12s
	require.Equal(t, int64(12*sec), d.f.NextLeaseExpiry())
	require.Equal(t, 0, d.reap(11*sec).Expired)
	require.Equal(t, 2, d.reap(12*sec).Expired)
	require.Equal(t, pb.TaskState_TASK_STATE_PENDING, d.state("a"))
}

func TestPoisonTaskEventuallyDies(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec) // 3 attempts
	now := 2 * sec
	for i := 0; i < 3; i++ {
		require.Len(t, d.lease("crashy", now, "q"), 1)
		now += 20 * sec
		d.reap(now)
	}
	require.Equal(t, pb.TaskState_TASK_STATE_DEAD, d.state("a"))
}

func TestExpireNodeReleasesOnlyThatNodesTasks(t *testing.T) {
	d := newDriver(t)
	for _, id := range []string{"a", "b", "c"} {
		d.submit(id, "q", sec)
	}
	d.lease("n1/0", 2*sec, "q") // takes all three, so split them by hand
	d.f.mu.Lock()
	d.f.tasks["a"].LeaseWorker = "n1/0"
	d.f.tasks["b"].LeaseWorker = "n10/0" // prefix trap: n1 must not match n10
	d.f.tasks["c"].LeaseWorker = "n2/0"
	d.f.mu.Unlock()

	r := d.do(&pb.Command{Cmd: &pb.Command_ExpireNode{ExpireNode: &pb.ExpireNodeCmd{NodeId: "n1", NowNs: 3 * sec}}}).(*ExpireNodeResult)
	require.Equal(t, 1, r.Released)
	require.Equal(t, pb.TaskState_TASK_STATE_PENDING, d.state("a"))
	require.Equal(t, pb.TaskState_TASK_STATE_LEASED, d.state("b"))
	require.Equal(t, pb.TaskState_TASK_STATE_LEASED, d.state("c"))
}

func TestRateLimitTokenBucket(t *testing.T) {
	d := newDriver(t)
	for i := 0; i < 10; i++ {
		d.submit(fmt.Sprint("t", i), "q", sec)
	}
	d.do(&pb.Command{Cmd: &pb.Command_SetQueueConfig{SetQueueConfig: &pb.SetQueueConfigCmd{
		Config: &pb.QueueConfig{Name: "q", RatePerSec: 2, Burst: 3}, NowNs: sec,
	}}})

	require.Len(t, d.lease("w", sec, "q"), 3, "burst")
	require.Empty(t, d.lease("w", sec+100*int64(time.Millisecond), "q"), "bucket empty")
	require.Len(t, d.lease("w", sec+600*int64(time.Millisecond), "q"), 1, "0.5s at 2/s refills one token")
}

func TestRateLimitSurvivesTinyPollIntervals(t *testing.T) {
	d := newDriver(t)
	for i := 0; i < 100; i++ {
		d.submit(fmt.Sprint("t", i), "q", sec)
	}
	d.do(&pb.Command{Cmd: &pb.Command_SetQueueConfig{SetQueueConfig: &pb.SetQueueConfigCmd{
		Config: &pb.QueueConfig{Name: "q", RatePerSec: 1, Burst: 1}, NowNs: sec,
	}}})
	got := len(d.lease("w", sec, "q"))
	// poll every 0.3ms for two seconds: refill must accumulate, not round away
	for now := sec; now < 3*sec; now += 300_000 {
		got += len(d.lease("w", now, "q"))
	}
	require.InDelta(t, 3, got, 1, "1 burst token + ~2 refilled over two seconds")
}

func TestMaxInFlight(t *testing.T) {
	d := newDriver(t)
	for i := 0; i < 5; i++ {
		d.submit(fmt.Sprint("t", i), "q", sec)
	}
	d.do(&pb.Command{Cmd: &pb.Command_SetQueueConfig{SetQueueConfig: &pb.SetQueueConfigCmd{
		Config: &pb.QueueConfig{Name: "q", MaxInFlight: 2}, NowNs: sec,
	}}})
	got := d.lease("w", sec, "q")
	require.Len(t, got, 2)
	require.Empty(t, d.lease("w", sec, "q"))
	d.complete(got[0].Id, got[0].LeaseToken, 2*sec)
	require.Len(t, d.lease("w", 2*sec, "q"), 1)
}

func TestCancel(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec)
	d.submit("b", "q", sec)
	r := d.do(&pb.Command{Cmd: &pb.Command_Cancel{Cancel: &pb.CancelCmd{TaskId: "a", NowNs: sec}}})
	require.Equal(t, pb.TaskState_TASK_STATE_CANCELED, r.(*TaskResult).Task.State)
	got := d.lease("w", 2*sec, "q")
	require.Len(t, got, 1)
	require.Equal(t, "b", got[0].Id)

	// cancelling a leased task fences its worker
	d.do(&pb.Command{Cmd: &pb.Command_Cancel{Cancel: &pb.CancelCmd{TaskId: "b", NowNs: 3 * sec}}})
	require.ErrorIs(t, asErr(d.complete("b", got[0].LeaseToken, 4*sec)), ErrFenced)
	require.ErrorIs(t, asErr(d.do(&pb.Command{Cmd: &pb.Command_Cancel{Cancel: &pb.CancelCmd{TaskId: "b", NowNs: 5 * sec}}})), ErrWrongState)
}

func TestRetentionCollectsFinishedTasks(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec, func(c *pb.SubmitCmd) { c.IdempotencyKey = "k" })
	tk := d.lease("w", sec, "q")[0]
	d.complete("a", tk.LeaseToken, 2*sec)
	r := d.do(&pb.Command{Cmd: &pb.Command_Reap{Reap: &pb.ReapCmd{NowNs: 100 * sec, RetentionNs: 50 * sec}}}).(*ReapResult)
	require.Equal(t, 1, r.Collected)
	require.Equal(t, 0, d.f.Len())
	// dedupe window closed with it
	require.False(t, d.submit("again", "q", 101*sec, func(c *pb.SubmitCmd) { c.IdempotencyKey = "k" }).(*SubmitResult).Duplicate)
}

func TestHasReadyDoesNotChangeReplicatedState(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec)
	d.submit("b", "q", sec, func(c *pb.SubmitCmd) { c.AvailableAtNs = 5 * sec })
	d.do(&pb.Command{Cmd: &pb.Command_Cancel{Cancel: &pb.CancelCmd{TaskId: "a", NowNs: sec}}})
	before := d.f.Digest()
	d.f.HasReady([]string{"q"}, 9*sec)
	require.Equal(t, before, d.f.Digest())
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	d := newDriver(t)
	d.submit("a", "q", sec)
	d.submit("b", "q", sec, func(c *pb.SubmitCmd) { c.AvailableAtNs = 50 * sec })
	d.submit("c", "q", sec)
	d.lease("w", 2*sec, "q")

	blob, err := d.f.Snapshot()
	require.NoError(t, err)
	other := New()
	require.NoError(t, other.Restore(blob))
	require.Equal(t, d.f.Digest(), other.Digest())

	// and it keeps behaving the same afterwards
	d2 := &driver{t: t, f: other, idx: d.idx}
	for _, dd := range []*driver{d, d2} {
		dd.reap(30 * sec)
		dd.lease("w2", 60*sec, "q")
	}
	require.Equal(t, d.f.Digest(), d2.f.Digest())
}

// Two replicas, one of which gets rebuilt from a snapshot halfway through,
// must agree on every result and on the final state for arbitrary command mixes.
func TestReplicasStayIdenticalUnderRandomCommands(t *testing.T) {
	for seed := int64(0); seed < 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		a, b := newDriver(t), newDriver(t)
		var live []*pb.Task // tasks a worker might still hold
		now := sec
		next := 0
		for step := 0; step < 400; step++ {
			now += int64(rng.Intn(3000)) * int64(time.Millisecond)
			var cmd *pb.Command
			switch rng.Intn(8) {
			case 0, 1:
				next++
				cmd = &pb.Command{Cmd: &pb.Command_Submit{Submit: &pb.SubmitCmd{
					Id: fmt.Sprint("t", next), Queue: []string{"q1", "q2"}[rng.Intn(2)], Type: "x", NowNs: now,
					Priority: int32(rng.Intn(3)), AvailableAtNs: now + int64(rng.Intn(4))*sec,
					Retry: &pb.RetryPolicy{MaxAttempts: uint32(1 + rng.Intn(3))},
				}}}
			case 2, 3:
				cmd = &pb.Command{Cmd: &pb.Command_Lease{Lease: &pb.LeaseCmd{
					WorkerId: fmt.Sprintf("n%d/0", rng.Intn(3)), Queues: []string{"q1", "q2"},
					MaxTasks: uint32(1 + rng.Intn(3)), TtlNs: int64(1+rng.Intn(5)) * sec, NowNs: now,
				}}}
			case 4:
				if len(live) > 0 {
					tk := live[rng.Intn(len(live))]
					cmd = &pb.Command{Cmd: &pb.Command_Complete{Complete: &pb.CompleteCmd{TaskId: tk.Id, Token: tk.LeaseToken, NowNs: now}}}
				}
			case 5:
				if len(live) > 0 {
					tk := live[rng.Intn(len(live))]
					cmd = &pb.Command{Cmd: &pb.Command_Fail{Fail: &pb.FailCmd{TaskId: tk.Id, Token: tk.LeaseToken, Error: "e", NowNs: now, RetryAtNs: now + sec}}}
				}
			case 6:
				cmd = &pb.Command{Cmd: &pb.Command_Reap{Reap: &pb.ReapCmd{NowNs: now, RetentionNs: 20 * sec}}}
			case 7:
				cmd = &pb.Command{Cmd: &pb.Command_ExpireNode{ExpireNode: &pb.ExpireNodeCmd{NodeId: fmt.Sprintf("n%d", rng.Intn(3)), NowNs: now}}}
			}
			if cmd == nil {
				continue
			}
			ra, rb := a.do(cmd), b.do(cmd)
			require.Equal(t, fmt.Sprintf("%v", ra), fmt.Sprintf("%v", rb), "seed %d step %d", seed, step)
			if lr, ok := ra.(*LeaseResult); ok {
				live = append(live, lr.Tasks...)
			}
			if step == 200 { // rebuild b from a snapshot
				blob, err := b.f.Snapshot()
				require.NoError(t, err)
				nb := New()
				require.NoError(t, nb.Restore(blob))
				b.f = nb
			}
		}
		require.Equal(t, a.f.Digest(), b.f.Digest(), "seed %d", seed)
	}
}

func BenchmarkSubmitLeaseComplete(b *testing.B) {
	f := New()
	var idx uint64
	apply := func(c *pb.Command) any {
		data, _ := proto.Marshal(c)
		idx++
		return f.Apply(idx, data)
	}
	now := sec
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now += 1000
		apply(&pb.Command{Cmd: &pb.Command_Submit{Submit: &pb.SubmitCmd{Id: fmt.Sprint("t", i), Queue: "q", Type: "x", NowNs: now, Retry: &pb.RetryPolicy{MaxAttempts: 3}}}})
		r := apply(&pb.Command{Cmd: &pb.Command_Lease{Lease: &pb.LeaseCmd{WorkerId: "w", Queues: []string{"q"}, MaxTasks: 1, TtlNs: sec, NowNs: now}}}).(*LeaseResult)
		apply(&pb.Command{Cmd: &pb.Command_Complete{Complete: &pb.CompleteCmd{TaskId: r.Tasks[0].Id, Token: r.Tasks[0].LeaseToken, NowNs: now}}})
	}
}
