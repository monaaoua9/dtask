package node_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/raft"
)

func TestEveryTaskRunsExactlyOnceWithoutFaults(t *testing.T) {
	c := newTestCluster(t, 3)
	c.leader()

	const n = 150
	for i := 0; i < n; i++ {
		c.submit(fmt.Sprint("job-", i), workSpec{Ms: 5})
	}
	c.waitFinished(n, 30*time.Second)

	execs := c.rec.all()
	require.Len(t, execs, n, "no fault injected, so no task may run twice")
	tokensUnique(t, execs)
	for _, task := range c.allTasks() {
		require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, task.State)
		require.Equal(t, uint32(1), task.Attempt)
	}
	c.assertConverged(5 * time.Second)
}

func TestSubmitIsIdempotent(t *testing.T) {
	c := newTestCluster(t, 3)
	c.leader()
	a := c.submit("same-key", workSpec{Ms: 1})
	b := c.submit("same-key", workSpec{Ms: 1})
	require.Equal(t, a.Id, b.Id)
	c.waitFinished(1, 10*time.Second)
	require.Len(t, c.rec.all(), 1)
}

func TestRetryWithBackoffThenSuccess(t *testing.T) {
	c := newTestCluster(t, 3)
	c.leader()
	task := c.submit("flaky", workSpec{Ms: 1, FailFirst: 2}, func(r *pb.SubmitRequest) {
		r.Retry = &pb.RetryPolicy{MaxAttempts: 5, BaseDelayMs: 200, MaxDelayMs: 2000}
	})
	c.waitFinished(1, 20*time.Second)

	got := c.allTasks()[0]
	require.Equal(t, task.Id, got.Id)
	require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, got.State)
	require.Equal(t, uint32(3), got.Attempt)

	ex := c.rec.byTask()[task.Id]
	require.Len(t, ex, 3)
	sort.Slice(ex, func(i, j int) bool { return ex[i].Start.Before(ex[j].Start) })
	t.Logf("backoff gaps: %v, %v", ex[1].Start.Sub(ex[0].End), ex[2].Start.Sub(ex[1].End))
}

func TestPoisonTaskGoesToDeadLetter(t *testing.T) {
	c := newTestCluster(t, 3)
	c.leader()
	c.submit("poison", workSpec{Ms: 1, AlwaysErr: true}, func(r *pb.SubmitRequest) {
		r.Retry = &pb.RetryPolicy{MaxAttempts: 3, BaseDelayMs: 20, MaxDelayMs: 100}
	})
	c.waitFinished(1, 20*time.Second)
	got := c.allTasks()[0]
	require.Equal(t, pb.TaskState_TASK_STATE_DEAD, got.State)
	require.Equal(t, uint32(3), got.Attempt)
	require.Contains(t, got.LastError, "simulated failure")
}

func TestLeaderCrashDoesNotLoseOrDuplicateWork(t *testing.T) {
	// lease TTL (30s) is much longer than the failover we expect: if the dead
	// node's tasks come back quickly it's the failure detector doing it
	c := newTestCluster(t, 5)
	c.leader()

	const n = 120
	for i := 0; i < n; i++ {
		c.submit(fmt.Sprint("job-", i), workSpec{Ms: 60})
	}
	require.Eventually(t, func() bool { _, _, leased, _, _ := c.counts(); return leased > 0 }, 5*time.Second, 5*time.Millisecond)

	c.crash(c.leader().ID())

	c.waitFinished(n, 40*time.Second)
	tasks := c.allTasks()
	require.Len(t, tasks, n)
	for _, task := range tasks {
		require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, task.State, task.Id)
	}
	tokensUnique(t, c.rec.all())
	c.assertConverged(5 * time.Second)
	t.Logf("executions=%d for %d tasks (extras are interrupted attempts)", len(c.rec.all()), n)
}

func TestFollowerCrashTasksAreRedistributedBeforeLeaseExpiry(t *testing.T) {
	c := newTestCluster(t, 3)
	l := c.leader()
	victim := c.someoneElse(l.ID())

	const n = 30
	for i := 0; i < n; i++ {
		c.submit(fmt.Sprint("job-", i), workSpec{Ms: 400})
	}
	require.Eventually(t, func() bool { return c.rec.startedOn(victim) }, 5*time.Second, 5*time.Millisecond, "victim never picked up work")

	c.crash(victim)
	c.waitFinished(n, 25*time.Second) // lease TTL is 30s, so lease expiry alone can't explain finishing
	for _, task := range c.allTasks() {
		require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, task.State)
	}
}

func (c *testCluster) someoneElse(not raft.NodeID) raft.NodeID {
	for _, id := range c.ids {
		if id != not {
			return id
		}
	}
	panic("no other node")
}

func TestZombieWorkerAfterPartitionIsFenced(t *testing.T) {
	c := newTestCluster(t, 3)
	l := c.leader()
	victim := c.someoneElse(l.ID())

	const n = 12
	for i := 0; i < n; i++ {
		c.submit(fmt.Sprint("job-", i), workSpec{Ms: 1200})
	}
	require.Eventually(t, func() bool { return c.rec.startedOn(victim) }, 5*time.Second, 5*time.Millisecond)

	c.isolate(victim) // alive and working, but cut off from the cluster
	c.waitFinished(n, 30*time.Second)

	// the zombie finishes and tries to report once the network is back
	c.net.Heal()
	time.Sleep(2 * time.Second)

	// the stored result must come from the execution that held the winning token
	byToken := map[uint64]execution{}
	for _, e := range c.rec.all() {
		byToken[e.Token] = e
	}
	for _, task := range c.allTasks() {
		require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, task.State)
		winner, ok := byToken[task.LeaseToken]
		require.True(t, ok, "no execution holds token %d", task.LeaseToken)
		require.Equal(t, "done:"+string(winner.Node), string(task.Result), task.Id)
	}
	reRun := 0
	for _, ex := range c.rec.byTask() {
		if len(ex) > 1 {
			reRun++
		}
	}
	require.Greater(t, reRun, 0, "the partitioned node's tasks should have been run again elsewhere")
	tokensUnique(t, c.rec.all())
	c.assertConverged(5 * time.Second)
}

func TestGracefulShutdownDrainsInFlightTasks(t *testing.T) {
	c := newTestCluster(t, 3)
	c.leader()
	const n = 24
	for i := 0; i < n; i++ {
		c.submit(fmt.Sprint("job-", i), workSpec{Ms: 300})
	}
	require.Eventually(t, func() bool { return c.rec.startedOn("n2") }, 5*time.Second, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, c.get("n2").Shutdown(ctx))
	c.net.Unregister("n2")

	c.waitFinished(n, 30*time.Second)
	for _, task := range c.allTasks() {
		require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, task.State)
		require.Equal(t, uint32(1), task.Attempt, "graceful drain shouldn't cost anyone an attempt: %s", task.Id)
	}
	require.Len(t, c.rec.all(), n, "nothing was interrupted, so nothing runs twice")
}

func TestShutdownPastDeadlineHandsTasksBack(t *testing.T) {
	c := newTestCluster(t, 3)
	c.leader()
	const n = 12
	for i := 0; i < n; i++ {
		c.submit(fmt.Sprint("job-", i), workSpec{Ms: 2000}, func(r *pb.SubmitRequest) {
			r.Retry = &pb.RetryPolicy{MaxAttempts: 1} // one shot: a burned attempt would kill the task
		})
	}
	require.Eventually(t, func() bool { return c.rec.startedOn("n3") }, 5*time.Second, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond) // far too short
	defer cancel()
	_ = c.get("n3").Shutdown(ctx)
	c.net.Unregister("n3")

	c.waitFinished(n, 60*time.Second)
	for _, task := range c.allTasks() {
		require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, task.State, "task %s (err=%q)", task.Id, task.LastError)
	}
}

func TestQueueRateLimitHoldsAcrossTheCluster(t *testing.T) {
	c := newTestCluster(t, 3)
	svc := c.leader().Service()
	_, err := svc.SetQueueConfig(context.Background(), &pb.SetQueueConfigRequest{Config: &pb.QueueConfig{Name: "default", RatePerSec: 20, Burst: 5}})
	require.NoError(t, err)

	const n = 60
	start := time.Now()
	for i := 0; i < n; i++ {
		c.submit(fmt.Sprint("job-", i), workSpec{Ms: 1})
	}
	c.waitFinished(n, 30*time.Second)

	// 5 burst + 20/s: 60 tasks can't finish in much under 2.75s
	require.Greater(t, time.Since(start), 2400*time.Millisecond, "rate limit not enforced")

	var starts []time.Time
	for _, e := range c.rec.all() {
		starts = append(starts, e.Start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for i := range starts {
		j := i
		for j < len(starts) && starts[j].Sub(starts[i]) < time.Second {
			j++
		}
		require.LessOrEqual(t, j-i, 20+5+3, "more than burst+rate task starts inside one second")
	}
}

func TestMembershipAddNodeAndKeepWorking(t *testing.T) {
	c := newTestCluster(t, 3)
	l := c.leader()
	for i := 0; i < 20; i++ {
		c.submit(fmt.Sprint("a-", i), workSpec{Ms: 2})
	}

	// bring up a fourth, empty node and add it through the leader
	cfg := c.cfg["n1"]
	cfg.ID, cfg.AdvertiseAddr, cfg.Bootstrap = "n4", "n4", nil
	cfg.DataDir = t.TempDir()
	cfg.Transport = c.net.Endpoint("n4")
	c.addMember("n4", cfg)
	c.start("n4")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := l.Service().AddServer(ctx, &pb.AddServerRequest{Id: "n4", Addr: "n4"})
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		c.submit(fmt.Sprint("b-", i), workSpec{Ms: 2})
	}
	c.waitFinished(40, 30*time.Second)
	c.assertConverged(10 * time.Second)
	require.Equal(t, 4, c.get("n4").Raft().Status().Config.Voters())
}
