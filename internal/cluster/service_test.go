package cluster_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/apierr"
	"github.com/monaaoua9/dtask/internal/cluster"
	"github.com/monaaoua9/dtask/internal/fsm"
	"github.com/monaaoua9/dtask/internal/raft"
	"github.com/monaaoua9/dtask/internal/raft/simnet"
)

// one real raft node + the real FSM, no network
func newService(t *testing.T, mut func(*cluster.Config)) (*cluster.Service, *fsm.FSM) {
	t.Helper()
	f := fsm.New()
	n, err := raft.New(raft.Config{
		ID: "n1", Addr: "n1", Bootstrap: []raft.Server{{ID: "n1", Addr: "n1", Voter: true}},
		Store: raft.NewMemStore(), Transport: simnet.New(1).Endpoint("n1"), FSM: f,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		HeartbeatInterval: 10 * time.Millisecond, ElectionTimeoutMin: 50 * time.Millisecond, ElectionTimeoutMax: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	n.Start()
	t.Cleanup(n.Stop)
	require.Eventually(t, n.IsLeader, 3*time.Second, 5*time.Millisecond)

	cfg := cluster.Config{NodeID: "n1"}
	if mut != nil {
		mut(&cfg)
	}
	return cluster.NewService(cfg, n, f), f
}

func TestSubmitValidation(t *testing.T) {
	svc, _ := newService(t, func(c *cluster.Config) { c.MaxPayload = 16 })
	ctx := context.Background()

	for name, req := range map[string]*pb.SubmitRequest{
		"empty queue":      {Type: "x"},
		"queue with space": {Queue: "a b", Type: "x"},
		"no type":          {Queue: "q"},
		"payload too big":  {Queue: "q", Type: "x", Payload: []byte(strings.Repeat("x", 17))},
		"negative delay":   {Queue: "q", Type: "x", DelayMs: -1},
		"absurd attempts":  {Queue: "q", Type: "x", Retry: &pb.RetryPolicy{MaxAttempts: 5000}},
	} {
		_, err := svc.Submit(ctx, req)
		require.ErrorIs(t, err, apierr.ErrInvalid, name)
	}

	resp, err := svc.Submit(ctx, &pb.SubmitRequest{Queue: "q", Type: "x"})
	require.NoError(t, err)
	require.Equal(t, uint32(5), resp.Task.Retry.MaxAttempts, "server-side retry defaults applied")
}

func TestLeaseHeartbeatCompleteFlow(t *testing.T) {
	svc, _ := newService(t, nil)
	ctx := context.Background()
	sub, err := svc.Submit(ctx, &pb.SubmitRequest{Queue: "q", Type: "x"})
	require.NoError(t, err)

	empty, err := svc.Lease(ctx, &pb.LeaseRequest{WorkerId: "w", Queues: []string{"nothing-here"}})
	require.NoError(t, err)
	require.Empty(t, empty.Tasks)

	got, err := svc.Lease(ctx, &pb.LeaseRequest{WorkerId: "w", Queues: []string{"q"}, LeaseTtlMs: 1000})
	require.NoError(t, err)
	require.Len(t, got.Tasks, 1)
	tk := got.Tasks[0]
	require.Equal(t, sub.Task.Id, tk.Id)

	ext, err := svc.ExtendLease(ctx, &pb.ExtendLeaseRequest{TaskId: tk.Id, Token: tk.LeaseToken, LeaseTtlMs: 5000})
	require.NoError(t, err)
	require.Greater(t, ext.ExpiresAtNs, tk.LeaseExpiresAtNs)

	_, err = svc.Complete(ctx, &pb.CompleteRequest{TaskId: tk.Id, Token: tk.LeaseToken + 1})
	require.ErrorIs(t, err, apierr.ErrFenced)
	_, err = svc.Complete(ctx, &pb.CompleteRequest{TaskId: tk.Id, Token: tk.LeaseToken, Result: []byte("r")})
	require.NoError(t, err)

	read, err := svc.GetTask(ctx, &pb.GetTaskRequest{Id: tk.Id})
	require.NoError(t, err)
	require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, read.Task.State)

	_, err = svc.GetTask(ctx, &pb.GetTaskRequest{Id: "nope"})
	require.ErrorIs(t, err, apierr.ErrNotFound)
}

func TestFailPicksBackoffWithinPolicy(t *testing.T) {
	svc, _ := newService(t, nil)
	ctx := context.Background()
	_, err := svc.Submit(ctx, &pb.SubmitRequest{Queue: "q", Type: "x", Retry: &pb.RetryPolicy{MaxAttempts: 3, BaseDelayMs: 2000, MaxDelayMs: 2000}})
	require.NoError(t, err)

	got, _ := svc.Lease(ctx, &pb.LeaseRequest{WorkerId: "w", Queues: []string{"q"}})
	before := time.Now()
	resp, err := svc.Fail(ctx, &pb.FailRequest{TaskId: got.Tasks[0].Id, Token: got.Tasks[0].LeaseToken, Error: "boom"})
	require.NoError(t, err)
	require.Equal(t, pb.TaskState_TASK_STATE_PENDING, resp.Task.State)

	delay := time.Duration(resp.Task.AvailableAtNs - before.UnixNano())
	require.Less(t, delay, 2*time.Second+100*time.Millisecond)
	require.GreaterOrEqual(t, delay, -100*time.Millisecond)
}

func TestListPaginationCursor(t *testing.T) {
	svc, _ := newService(t, nil)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err := svc.Submit(ctx, &pb.SubmitRequest{Queue: "q", Type: "x"})
		require.NoError(t, err)
	}
	p1, err := svc.ListTasks(ctx, &pb.ListTasksRequest{PageSize: 2})
	require.NoError(t, err)
	require.Len(t, p1.Tasks, 2)
	require.NotEmpty(t, p1.NextCursor)
	p2, _ := svc.ListTasks(ctx, &pb.ListTasksRequest{PageSize: 2, Cursor: p1.NextCursor})
	p3, _ := svc.ListTasks(ctx, &pb.ListTasksRequest{PageSize: 2, Cursor: p2.NextCursor})
	require.Len(t, p3.Tasks, 1)
	require.Empty(t, p3.NextCursor)
	require.NotEqual(t, p1.Tasks[1].Id, p2.Tasks[0].Id)

	_, err = svc.ListTasks(ctx, &pb.ListTasksRequest{Cursor: "garbage"})
	require.ErrorIs(t, err, apierr.ErrInvalid)
}

func TestStatusReportsSelf(t *testing.T) {
	svc, _ := newService(t, nil)
	st, err := svc.Status(context.Background(), &pb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, "leader", st.Role)
	require.Len(t, st.Servers, 1)
	require.True(t, st.Servers[0].Healthy)
}
