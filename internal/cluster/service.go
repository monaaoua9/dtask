// Package cluster is the leader-side application layer: it validates
// requests, turns them into deterministic commands (stamping the clock and the
// retry delay, which replicas must not decide for themselves), sends them
// through Raft and reads results back out of the FSM.
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/apierr"
	"github.com/monaaoua9/dtask/internal/backoff"
	"github.com/monaaoua9/dtask/internal/fsm"
	"github.com/monaaoua9/dtask/internal/raft"
)

type Config struct {
	NodeID       raft.NodeID
	DefaultLease time.Duration
	MaxLease     time.Duration
	DefaultRetry *pb.RetryPolicy
	MaxPayload   int
	Clock        func() time.Time
	Metrics      *Metrics
	// Health reports the failure detector's view of a peer. Optional.
	Health func(id raft.NodeID) (phi float64, healthy bool)
}

func (c *Config) setDefaults() {
	if c.DefaultLease == 0 {
		c.DefaultLease = 30 * time.Second
	}
	if c.MaxLease == 0 {
		c.MaxLease = 10 * time.Minute
	}
	if c.DefaultRetry == nil {
		c.DefaultRetry = &pb.RetryPolicy{MaxAttempts: 5, BaseDelayMs: 1000, MaxDelayMs: 300_000}
	}
	if c.MaxPayload == 0 {
		c.MaxPayload = 1 << 20
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
}

// InvalidError marks a request the caller got wrong.
type InvalidError struct{ Msg string }

func (e *InvalidError) Error() string { return e.Msg }

func (e *InvalidError) Is(target error) bool { return target == apierr.ErrInvalid }

func invalid(format string, a ...any) error { return &InvalidError{fmt.Sprintf(format, a...)} }

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Service struct {
	pb.UnimplementedTaskServiceServer
	pb.UnimplementedAdminServiceServer

	cfg  Config
	raft *raft.Node
	fsm  *fsm.FSM
	rng  *backoff.Rand
}

func NewService(cfg Config, n *raft.Node, f *fsm.FSM) *Service {
	cfg.setDefaults()
	var seed int64
	_ = binary.Read(rand.Reader, binary.LittleEndian, &seed)
	return &Service{cfg: cfg, raft: n, fsm: f, rng: backoff.NewRand(seed)}
}

func (s *Service) now() time.Time { return s.cfg.Clock() }

func (s *Service) apply(ctx context.Context, c *pb.Command) (any, error) {
	b, err := proto.Marshal(c)
	if err != nil {
		return nil, err
	}
	res, err := s.raft.Propose(ctx, b)
	if err != nil {
		return nil, err
	}
	if e, ok := res.(error); ok {
		return nil, e
	}
	return res, nil
}

func (s *Service) notLeader() error {
	st := s.raft.Status()
	return &raft.NotLeaderError{LeaderID: st.LeaderID, LeaderAddr: st.LeaderAddr}
}

func newID(now time.Time) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(now.UnixMilli())<<16) // time-ordered prefix
	_, _ = rand.Read(b[6:])
	return hex.EncodeToString(b[:])
}

func (s *Service) Submit(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	if !nameRe.MatchString(req.Queue) {
		return nil, invalid("queue name must match %s", nameRe)
	}
	if req.Type == "" || len(req.Type) > 128 {
		return nil, invalid("type is required and must be at most 128 bytes")
	}
	if len(req.Payload) > s.cfg.MaxPayload {
		return nil, invalid("payload is %d bytes, limit is %d", len(req.Payload), s.cfg.MaxPayload)
	}
	if req.DelayMs < 0 {
		return nil, invalid("delay_ms can't be negative")
	}
	if len(req.IdempotencyKey) > 256 {
		return nil, invalid("idempotency_key too long")
	}
	retry, err := s.normalizeRetry(req.Retry)
	if err != nil {
		return nil, err
	}

	now := s.now()
	res, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_Submit{Submit: &pb.SubmitCmd{
		Id: newID(now), Queue: req.Queue, Type: req.Type, Payload: req.Payload, Priority: req.Priority,
		Retry: retry, IdempotencyKey: req.IdempotencyKey,
		AvailableAtNs: now.Add(time.Duration(req.DelayMs) * time.Millisecond).UnixNano(),
		NowNs:         now.UnixNano(),
	}}})
	if err != nil {
		return nil, err
	}
	r := res.(*fsm.SubmitResult)
	if !r.Duplicate {
		s.cfg.Metrics.submitted(req.Queue)
	}
	return &pb.SubmitResponse{Task: r.Task, Duplicate: r.Duplicate}, nil
}

func (s *Service) normalizeRetry(r *pb.RetryPolicy) (*pb.RetryPolicy, error) {
	out := proto.Clone(s.cfg.DefaultRetry).(*pb.RetryPolicy)
	if r == nil {
		return out, nil
	}
	if r.MaxAttempts > 0 {
		out.MaxAttempts = r.MaxAttempts
	}
	if r.BaseDelayMs > 0 {
		out.BaseDelayMs = r.BaseDelayMs
	}
	if r.MaxDelayMs > 0 {
		out.MaxDelayMs = r.MaxDelayMs
	}
	if out.MaxAttempts > 1000 {
		return nil, invalid("max_attempts above 1000")
	}
	if out.MaxDelayMs < out.BaseDelayMs {
		out.MaxDelayMs = out.BaseDelayMs
	}
	return out, nil
}

func (s *Service) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.GetTaskResponse, error) {
	if err := s.raft.ReadBarrier(ctx); err != nil {
		return nil, err
	}
	t, ok := s.fsm.Get(req.Id)
	if !ok {
		return nil, fsm.ErrNotFound
	}
	return &pb.GetTaskResponse{Task: t}, nil
}

func (s *Service) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	var after uint64
	if req.Cursor != "" {
		v, err := strconv.ParseUint(req.Cursor, 10, 64)
		if err != nil {
			return nil, invalid("bad cursor")
		}
		after = v
	}
	size := int(req.PageSize)
	if size <= 0 {
		size = 100
	}
	if size > 1000 {
		size = 1000
	}
	if err := s.raft.ReadBarrier(ctx); err != nil {
		return nil, err
	}
	page, more := s.fsm.List(req.Queue, req.State, size, after)
	resp := &pb.ListTasksResponse{Tasks: page}
	if more {
		resp.NextCursor = strconv.FormatUint(page[len(page)-1].Seq, 10)
	}
	return resp, nil
}

func (s *Service) CancelTask(ctx context.Context, req *pb.CancelTaskRequest) (*pb.CancelTaskResponse, error) {
	res, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_Cancel{Cancel: &pb.CancelCmd{TaskId: req.Id, NowNs: s.now().UnixNano()}}})
	if err != nil {
		return nil, err
	}
	return &pb.CancelTaskResponse{Task: res.(*fsm.TaskResult).Task}, nil
}

func (s *Service) Lease(ctx context.Context, req *pb.LeaseRequest) (*pb.LeaseResponse, error) {
	if req.WorkerId == "" || len(req.Queues) == 0 {
		return nil, invalid("worker_id and at least one queue are required")
	}
	if !s.raft.IsLeader() {
		return nil, s.notLeader()
	}
	ttl := time.Duration(req.LeaseTtlMs) * time.Millisecond
	if ttl <= 0 {
		ttl = s.cfg.DefaultLease
	}
	if ttl > s.cfg.MaxLease {
		ttl = s.cfg.MaxLease
	}
	now := s.now()

	// polling an empty queue shouldn't write to the replicated log
	if !s.fsm.HasReady(req.Queues, now.UnixNano()) {
		return &pb.LeaseResponse{}, nil
	}
	res, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_Lease{Lease: &pb.LeaseCmd{
		WorkerId: req.WorkerId, Queues: req.Queues, MaxTasks: req.MaxTasks,
		TtlNs: ttl.Nanoseconds(), NowNs: now.UnixNano(),
	}}})
	if err != nil {
		return nil, err
	}
	tasks := res.(*fsm.LeaseResult).Tasks
	for _, t := range tasks {
		s.cfg.Metrics.leased(t.Queue, time.Duration(now.UnixNano()-t.AvailableAtNs))
	}
	return &pb.LeaseResponse{Tasks: tasks}, nil
}

func (s *Service) ExtendLease(ctx context.Context, req *pb.ExtendLeaseRequest) (*pb.ExtendLeaseResponse, error) {
	ttl := time.Duration(req.LeaseTtlMs) * time.Millisecond
	if ttl <= 0 {
		ttl = s.cfg.DefaultLease
	}
	if ttl > s.cfg.MaxLease {
		ttl = s.cfg.MaxLease
	}
	res, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_Extend{Extend: &pb.ExtendCmd{
		TaskId: req.TaskId, Token: req.Token, TtlNs: ttl.Nanoseconds(), NowNs: s.now().UnixNano(),
	}}})
	if err != nil {
		s.noteFenced(err)
		return nil, err
	}
	return &pb.ExtendLeaseResponse{ExpiresAtNs: res.(*fsm.ExtendResult).ExpiresAtNs}, nil
}

func (s *Service) Complete(ctx context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	_, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_Complete{Complete: &pb.CompleteCmd{
		TaskId: req.TaskId, Token: req.Token, Result: req.Result, NowNs: s.now().UnixNano(),
	}}})
	if err != nil {
		s.noteFenced(err)
		return nil, err
	}
	s.cfg.Metrics.outcome("succeeded")
	return &pb.CompleteResponse{}, nil
}

func (s *Service) Fail(ctx context.Context, req *pb.FailRequest) (*pb.FailResponse, error) {
	if !s.raft.IsLeader() {
		return nil, s.notLeader()
	}
	t, ok := s.fsm.Get(req.TaskId)
	if !ok {
		return nil, fsm.ErrNotFound
	}
	now := s.now()
	// the backoff delay is picked here, once, and written into the command;
	// replicas replaying the log must never roll their own dice
	pol := backoff.Policy{
		Base: time.Duration(t.Retry.GetBaseDelayMs()) * time.Millisecond,
		Max:  time.Duration(t.Retry.GetMaxDelayMs()) * time.Millisecond,
	}
	retryAt := now.Add(pol.Delay(int(t.Attempt)-1, s.rng))

	res, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_Fail{Fail: &pb.FailCmd{
		TaskId: req.TaskId, Token: req.Token, Error: req.Error, Permanent: req.Permanent,
		RetryAtNs: retryAt.UnixNano(), NowNs: now.UnixNano(),
	}}})
	if err != nil {
		s.noteFenced(err)
		return nil, err
	}
	out := res.(*fsm.TaskResult).Task
	if out.State == pb.TaskState_TASK_STATE_DEAD {
		s.cfg.Metrics.outcome("dead")
	} else {
		s.cfg.Metrics.outcome("retried")
	}
	return &pb.FailResponse{Task: out}, nil
}

func (s *Service) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error) {
	_, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_Release{Release: &pb.ReleaseCmd{
		TaskId: req.TaskId, Token: req.Token, NowNs: s.now().UnixNano(),
	}}})
	if err != nil {
		s.noteFenced(err)
		return nil, err
	}
	s.cfg.Metrics.outcome("released")
	return &pb.ReleaseResponse{}, nil
}

func (s *Service) noteFenced(err error) {
	if errors.Is(err, fsm.ErrFenced) {
		s.cfg.Metrics.fenced()
	}
}

// Reap is called by the scheduler loop: it requeues expired leases and, when
// retention is set, garbage-collects old finished tasks.
func (s *Service) Reap(ctx context.Context, retention time.Duration) (fsm.ReapResult, error) {
	res, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_Reap{Reap: &pb.ReapCmd{
		NowNs: s.now().UnixNano(), RetentionNs: retention.Nanoseconds(),
	}}})
	if err != nil {
		return fsm.ReapResult{}, err
	}
	r := *res.(*fsm.ReapResult)
	s.cfg.Metrics.reaped(r.Expired, r.Collected)
	return r, nil
}

// ExpireNode returns every task held by the node's workers to the queue.
func (s *Service) ExpireNode(ctx context.Context, id raft.NodeID) (int, error) {
	res, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_ExpireNode{ExpireNode: &pb.ExpireNodeCmd{
		NodeId: string(id), NowNs: s.now().UnixNano(),
	}}})
	if err != nil {
		return 0, err
	}
	n := res.(*fsm.ExpireNodeResult).Released
	s.cfg.Metrics.nodeExpired(n)
	return n, nil
}

func (s *Service) Status(_ context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	st := s.raft.Status()
	resp := &pb.StatusResponse{
		NodeId: string(st.ID), Role: st.Role.String(), Term: st.Term,
		LeaderId: string(st.LeaderID), LeaderAddr: st.LeaderAddr,
		CommitIndex: st.CommitIndex, LastApplied: st.LastApplied, LastLogIndex: st.LastLogIndex,
	}
	for _, sv := range st.Config.Servers {
		info := &pb.ServerInfo{Id: string(sv.ID), Addr: sv.Addr, Voter: sv.Voter}
		if st.Role == raft.Leader && s.cfg.Health != nil && sv.ID != st.ID {
			info.Phi, info.Healthy = s.cfg.Health(sv.ID)
		} else if sv.ID == st.ID {
			info.Healthy = true
		}
		resp.Servers = append(resp.Servers, info)
	}
	return resp, nil
}

func (s *Service) AddServer(ctx context.Context, req *pb.AddServerRequest) (*pb.AddServerResponse, error) {
	if !nameRe.MatchString(req.Id) || req.Addr == "" {
		return nil, invalid("id and addr are required")
	}
	if err := s.raft.AddServer(ctx, raft.NodeID(req.Id), req.Addr); err != nil {
		return nil, err
	}
	return &pb.AddServerResponse{}, nil
}

func (s *Service) RemoveServer(ctx context.Context, req *pb.RemoveServerRequest) (*pb.RemoveServerResponse, error) {
	if err := s.raft.RemoveServer(ctx, raft.NodeID(req.Id)); err != nil {
		return nil, err
	}
	return &pb.RemoveServerResponse{}, nil
}

func (s *Service) SetQueueConfig(ctx context.Context, req *pb.SetQueueConfigRequest) (*pb.SetQueueConfigResponse, error) {
	c := req.GetConfig()
	if c == nil || !nameRe.MatchString(c.Name) {
		return nil, invalid("config.name is required")
	}
	if c.RatePerSec < 0 || c.RatePerSec > 1e6 {
		return nil, invalid("rate_per_sec out of range")
	}
	if _, err := s.apply(ctx, &pb.Command{Cmd: &pb.Command_SetQueueConfig{SetQueueConfig: &pb.SetQueueConfigCmd{
		Config: c, NowNs: s.now().UnixNano(),
	}}}); err != nil {
		return nil, err
	}
	return &pb.SetQueueConfigResponse{}, nil
}

func (s *Service) QueueStats(ctx context.Context, _ *pb.QueueStatsRequest) (*pb.QueueStatsResponse, error) {
	if err := s.raft.ReadBarrier(ctx); err != nil {
		return nil, err
	}
	var out pb.QueueStatsResponse
	for _, q := range s.fsm.Stats(s.now().UnixNano()) {
		out.Queues = append(out.Queues, &pb.QueueStat{
			Name: q.Name, Pending: q.Pending, Delayed: q.Delayed, Leased: q.Leased,
			Succeeded: q.Succeeded, Dead: q.Dead, Config: q.Config,
		})
	}
	return &out, nil
}
