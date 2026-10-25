// Package simnet is an in-process network for tests. Nodes talk through it
// like they would over gRPC, but the test decides which links work: it can
// cut nodes off, split the cluster, add latency and drop messages.
package simnet

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/monaaoua9/dtask/internal/raft"
)

var ErrUnreachable = errors.New("simnet: unreachable")

type link struct{ from, to raft.NodeID }

type Network struct {
	mu       sync.RWMutex
	handlers map[raft.NodeID]raft.RPCHandler
	blocked  map[link]bool
	minDelay time.Duration
	maxDelay time.Duration
	dropRate float64
	rng      *rand.Rand
	rngMu    sync.Mutex
}

func New(seed int64) *Network {
	return &Network{
		handlers: map[raft.NodeID]raft.RPCHandler{},
		blocked:  map[link]bool{},
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (n *Network) Register(id raft.NodeID, h raft.RPCHandler) {
	n.mu.Lock()
	n.handlers[id] = h
	n.mu.Unlock()
}

// Unregister makes a node unreachable, like a process that died.
func (n *Network) Unregister(id raft.NodeID) {
	n.mu.Lock()
	delete(n.handlers, id)
	n.mu.Unlock()
}

// Partition splits the listed groups from each other. Nodes inside a group
// keep talking. Anything not listed is left as it was.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}
			for _, x := range a {
				for _, y := range b {
					n.blocked[link{x, y}] = true
				}
			}
		}
	}
}

// Isolate cuts one node off from everybody, in both directions.
func (n *Network) Isolate(id raft.NodeID, others []raft.NodeID) {
	n.Partition([]raft.NodeID{id}, others)
}

// BlockOneWay drops traffic from -> to only, the nastiest kind of partition.
func (n *Network) BlockOneWay(from, to raft.NodeID) {
	n.mu.Lock()
	n.blocked[link{from, to}] = true
	n.mu.Unlock()
}

func (n *Network) Heal() {
	n.mu.Lock()
	n.blocked = map[link]bool{}
	n.mu.Unlock()
}

func (n *Network) SetLatency(min, max time.Duration) {
	n.mu.Lock()
	n.minDelay, n.maxDelay = min, max
	n.mu.Unlock()
}

func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	n.dropRate = p
	n.mu.Unlock()
}

func (n *Network) Endpoint(from raft.NodeID) raft.Transport {
	return &endpoint{net: n, from: from}
}

func (n *Network) route(from, to raft.NodeID) (raft.RPCHandler, time.Duration, bool) {
	n.mu.RLock()
	h, ok := n.handlers[to]
	blocked := n.blocked[link{from, to}]
	lo, hi, drop := n.minDelay, n.maxDelay, n.dropRate
	n.mu.RUnlock()
	if !ok || blocked {
		return nil, 0, false
	}
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	if drop > 0 && n.rng.Float64() < drop {
		return nil, 0, false
	}
	d := lo
	if hi > lo {
		d += time.Duration(n.rng.Int63n(int64(hi - lo)))
	}
	return h, d, true
}

// Reachable says whether a message from -> to would currently be delivered
// (ignoring random loss). Handy for routing non-raft test traffic through the same faults.
func (n *Network) Reachable(from, to raft.NodeID) bool { return n.reachable(from, to) }

func (n *Network) reachable(from, to raft.NodeID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, ok := n.handlers[to]
	return ok && !n.blocked[link{from, to}]
}

type endpoint struct {
	net  *Network
	from raft.NodeID
}

// call models one request/response exchange. A lost message in either
// direction looks like a timeout to the caller, same as on a real network.
func call[T any](ctx context.Context, e *endpoint, to raft.NodeID, fn func(raft.RPCHandler) (*T, error)) (*T, error) {
	h, delay, ok := e.net.route(e.from, to)
	if !ok {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := sleep(ctx, delay); err != nil {
		return nil, err
	}
	resp, err := fn(h)
	if err != nil {
		return nil, err
	}
	if !e.net.reachable(to, e.from) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return resp, sleep(ctx, delay)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *endpoint) RequestVote(ctx context.Context, to raft.Server, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	r := *req
	return call(ctx, e, to.ID, func(h raft.RPCHandler) (*raft.RequestVoteResponse, error) {
		return h.HandleRequestVote(&r), nil
	})
}

func (e *endpoint) AppendEntries(ctx context.Context, to raft.Server, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	r := *req
	r.Entries = append([]raft.Entry(nil), req.Entries...)
	return call(ctx, e, to.ID, func(h raft.RPCHandler) (*raft.AppendEntriesResponse, error) {
		return h.HandleAppendEntries(&r)
	})
}

func (e *endpoint) InstallSnapshot(ctx context.Context, to raft.Server, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	r := *req
	r.Configuration = req.Configuration.Clone()
	return call(ctx, e, to.ID, func(h raft.RPCHandler) (*raft.InstallSnapshotResponse, error) {
		return h.HandleInstallSnapshot(&r)
	})
}

func (e *endpoint) TimeoutNow(ctx context.Context, to raft.Server, req *raft.TimeoutNowRequest) (*raft.TimeoutNowResponse, error) {
	r := *req
	return call(ctx, e, to.ID, func(h raft.RPCHandler) (*raft.TimeoutNowResponse, error) {
		return h.HandleTimeoutNow(&r)
	})
}

// Blocked lists the currently cut links, for diagnostics.
func (n *Network) Blocked() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var out []string
	for l := range n.blocked {
		out = append(out, string(l.from)+"->"+string(l.to))
	}
	sort.Strings(out)
	return out
}
