// Package grpctransport carries Raft RPCs over gRPC/protobuf.
package grpctransport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/raft"
)

const maxMsg = 256 << 20 // snapshots travel in one message

// Transport is the client half: it dials peers lazily and keeps one
// connection per address.
type Transport struct {
	creds credentials.TransportCredentials

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	cli   map[string]pb.RaftServiceClient
}

func NewTransport(creds credentials.TransportCredentials) *Transport {
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	return &Transport{creds: creds, conns: map[string]*grpc.ClientConn{}, cli: map[string]pb.RaftServiceClient{}}
}

func (t *Transport) client(addr string) (pb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.cli[addr]; ok {
		return c, nil
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(t.creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsg), grpc.MaxCallSendMsgSize(maxMsg)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second, PermitWithoutStream: true}),
		grpc.WithConnectParams(grpc.ConnectParams{
			// a restarting peer should be retried quickly, raft has its own backoff above us
			Backoff:           backoffCfg(),
			MinConnectTimeout: time.Second,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	c := pb.NewRaftServiceClient(cc)
	t.conns[addr], t.cli[addr] = cc, c
	return c, nil
}

func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, cc := range t.conns {
		_ = cc.Close()
	}
	t.conns, t.cli = map[string]*grpc.ClientConn{}, map[string]pb.RaftServiceClient{}
}

func (t *Transport) RequestVote(ctx context.Context, to raft.Server, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	c, err := t.client(to.Addr)
	if err != nil {
		return nil, err
	}
	r, err := c.RequestVote(ctx, &pb.RequestVoteRequest{
		Term: req.Term, CandidateId: string(req.CandidateID),
		LastLogIndex: req.LastLogIndex, LastLogTerm: req.LastLogTerm, PreVote: req.PreVote,
		LeadershipTransfer: req.LeadershipTransfer,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteResponse{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

func (t *Transport) AppendEntries(ctx context.Context, to raft.Server, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	c, err := t.client(to.Addr)
	if err != nil {
		return nil, err
	}
	out := &pb.AppendEntriesRequest{
		Term: req.Term, LeaderId: string(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex, PrevLogTerm: req.PrevLogTerm, LeaderCommit: req.LeaderCommit,
		Entries: make([]*pb.LogEntry, len(req.Entries)),
	}
	for i, e := range req.Entries {
		out.Entries[i] = &pb.LogEntry{Index: e.Index, Term: e.Term, Type: pb.EntryType(e.Type), Data: e.Data}
	}
	r, err := c.AppendEntries(ctx, out)
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesResponse{
		Term: r.Term, Success: r.Success, MatchIndex: r.MatchIndex,
		ConflictIndex: r.ConflictIndex, ConflictTerm: r.ConflictTerm,
	}, nil
}

func (t *Transport) InstallSnapshot(ctx context.Context, to raft.Server, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	c, err := t.client(to.Addr)
	if err != nil {
		return nil, err
	}
	r, err := c.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{
		Term: req.Term, LeaderId: string(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm,
		Configuration: raft.EncodeConfiguration(req.Configuration), Data: req.Data,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotResponse{Term: r.Term}, nil
}

func (t *Transport) TimeoutNow(ctx context.Context, to raft.Server, req *raft.TimeoutNowRequest) (*raft.TimeoutNowResponse, error) {
	c, err := t.client(to.Addr)
	if err != nil {
		return nil, err
	}
	r, err := c.TimeoutNow(ctx, &pb.TimeoutNowRequest{Term: req.Term, LeaderId: string(req.LeaderID)})
	if err != nil {
		return nil, err
	}
	return &raft.TimeoutNowResponse{Term: r.Term}, nil
}

var _ raft.Transport = (*Transport)(nil)

// Server is the receiving half, adapting protobuf messages to a raft.RPCHandler.
type Server struct {
	pb.UnimplementedRaftServiceServer
	H raft.RPCHandler
}

func (s *Server) RequestVote(_ context.Context, r *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	resp := s.H.HandleRequestVote(&raft.RequestVoteRequest{
		Term: r.Term, CandidateID: raft.NodeID(r.CandidateId),
		LastLogIndex: r.LastLogIndex, LastLogTerm: r.LastLogTerm, PreVote: r.PreVote,
		LeadershipTransfer: r.LeadershipTransfer,
	})
	return &pb.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (s *Server) AppendEntries(_ context.Context, r *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	req := &raft.AppendEntriesRequest{
		Term: r.Term, LeaderID: raft.NodeID(r.LeaderId),
		PrevLogIndex: r.PrevLogIndex, PrevLogTerm: r.PrevLogTerm, LeaderCommit: r.LeaderCommit,
		Entries: make([]raft.Entry, len(r.Entries)),
	}
	for i, e := range r.Entries {
		req.Entries[i] = raft.Entry{Index: e.Index, Term: e.Term, Type: raft.EntryType(e.Type), Data: e.Data}
	}
	resp, err := s.H.HandleAppendEntries(req)
	if err != nil {
		return nil, err
	}
	return &pb.AppendEntriesResponse{
		Term: resp.Term, Success: resp.Success, MatchIndex: resp.MatchIndex,
		ConflictIndex: resp.ConflictIndex, ConflictTerm: resp.ConflictTerm,
	}, nil
}

func (s *Server) InstallSnapshot(_ context.Context, r *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	conf, err := raft.DecodeConfiguration(r.Configuration)
	if err != nil {
		return nil, err
	}
	resp, err := s.H.HandleInstallSnapshot(&raft.InstallSnapshotRequest{
		Term: r.Term, LeaderID: raft.NodeID(r.LeaderId),
		LastIncludedIndex: r.LastIncludedIndex, LastIncludedTerm: r.LastIncludedTerm,
		Configuration: conf, Data: r.Data,
	})
	if err != nil {
		return nil, err
	}
	return &pb.InstallSnapshotResponse{Term: resp.Term}, nil
}

func (s *Server) TimeoutNow(_ context.Context, r *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	resp, err := s.H.HandleTimeoutNow(&raft.TimeoutNowRequest{Term: r.Term, LeaderID: raft.NodeID(r.LeaderId)})
	if err != nil {
		return nil, err
	}
	return &pb.TimeoutNowResponse{Term: resp.Term}, nil
}
