package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type NodeID string

type Role int32

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

type EntryType uint8

const (
	EntryCommand EntryType = iota + 1
	EntryNoop
	EntryConfig
)

type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

type Server struct {
	ID    NodeID `json:"id"`
	Addr  string `json:"addr"`
	Voter bool   `json:"voter"`
}

// Configuration is the cluster membership. Servers stay sorted by ID so two
// nodes that agree on membership also agree on the encoded bytes.
type Configuration struct {
	Servers []Server `json:"servers"`
}

func (c Configuration) Clone() Configuration {
	out := Configuration{Servers: make([]Server, len(c.Servers))}
	copy(out.Servers, c.Servers)
	return out
}

func (c Configuration) Get(id NodeID) (Server, bool) {
	for _, s := range c.Servers {
		if s.ID == id {
			return s, true
		}
	}
	return Server{}, false
}

func (c Configuration) IsVoter(id NodeID) bool {
	s, ok := c.Get(id)
	return ok && s.Voter
}

func (c Configuration) Voters() int {
	n := 0
	for _, s := range c.Servers {
		if s.Voter {
			n++
		}
	}
	return n
}

func (c Configuration) Quorum() int { return c.Voters()/2 + 1 }

func (c Configuration) with(s Server) Configuration {
	out := c.Clone()
	for i := range out.Servers {
		if out.Servers[i].ID == s.ID {
			out.Servers[i] = s
			return out
		}
	}
	out.Servers = append(out.Servers, s)
	sort.Slice(out.Servers, func(i, j int) bool { return out.Servers[i].ID < out.Servers[j].ID })
	return out
}

func (c Configuration) without(id NodeID) Configuration {
	out := Configuration{}
	for _, s := range c.Servers {
		if s.ID != id {
			out.Servers = append(out.Servers, s)
		}
	}
	return out
}

func EncodeConfiguration(c Configuration) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // plain struct of strings and bools
	}
	return b
}

func DecodeConfiguration(b []byte) (Configuration, error) {
	var c Configuration
	if err := json.Unmarshal(b, &c); err != nil {
		return Configuration{}, fmt.Errorf("decode config: %w", err)
	}
	return c, nil
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
	// election started by a leadership transfer: skip leader stickiness
	LeadershipTransfer bool
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64
	ConflictIndex uint64
	ConflictTerm  uint64
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Configuration     Configuration
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term uint64
}

type TimeoutNowRequest struct {
	Term     uint64
	LeaderID NodeID
}

type TimeoutNowResponse struct {
	Term uint64
}

// Transport moves RPCs between nodes. Implementations must be safe for
// concurrent use and honour ctx cancellation.
type Transport interface {
	RequestVote(ctx context.Context, to Server, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to Server, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to Server, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
	TimeoutNow(ctx context.Context, to Server, req *TimeoutNowRequest) (*TimeoutNowResponse, error)
}

// RPCHandler is what a Node exposes to the transport layer.
type RPCHandler interface {
	HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
	HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	HandleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
	HandleTimeoutNow(req *TimeoutNowRequest) (*TimeoutNowResponse, error)
}

// FSM is the replicated state machine. Apply is called sequentially in log
// order from a single goroutine; its return value is handed back to whoever
// proposed the command (on the leader only).
type FSM interface {
	Apply(index uint64, data []byte) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

type SnapshotMeta struct {
	Index         uint64
	Term          uint64
	Configuration Configuration
}

type PersistentState struct {
	Term     uint64
	VotedFor NodeID
}

// Store persists everything a node must not forget across restarts. Methods
// are called with the node lock held, so they should be quick (fsync is fine).
type Store interface {
	// Load returns whatever was saved before; zero values on a fresh store.
	Load() (PersistentState, []Entry, *SnapshotMeta, []byte, error)
	SaveState(PersistentState) error
	// Append writes entries after the current last one, synced before return.
	Append([]Entry) error
	// TruncateSuffix drops every entry with index >= from.
	TruncateSuffix(from uint64) error
	// SaveSnapshot persists the snapshot and drops log entries <= meta.Index.
	SaveSnapshot(meta SnapshotMeta, data []byte) error
	Close() error
}

var (
	ErrNotLeader        = errors.New("raft: not the leader")
	ErrLeadershipLost   = errors.New("raft: leadership lost before commit, outcome unknown")
	ErrStopped          = errors.New("raft: node stopped")
	ErrConfigInProgress = errors.New("raft: another membership change is in progress")
	ErrLeaderNotReady   = errors.New("raft: leader has not committed an entry in its term yet")
	ErrUnknownServer    = errors.New("raft: unknown server")
	ErrLastVoter        = errors.New("raft: cannot remove the last voter")
	ErrTransferring     = errors.New("raft: leadership transfer in progress")
	ErrNoTransferTarget = errors.New("raft: no other voter to hand leadership to")
	ErrSnapshotStale    = errors.New("raft: snapshot is older than local state")
)

// NotLeaderError carries a hint for clients to retry elsewhere.
type NotLeaderError struct {
	LeaderID   NodeID
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "raft: not the leader, no leader known"
	}
	return fmt.Sprintf("raft: not the leader, try %s (%s)", e.LeaderID, e.LeaderAddr)
}

func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }
