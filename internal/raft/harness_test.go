package raft_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/monaaoua9/dtask/internal/raft"
	"github.com/monaaoua9/dtask/internal/raft/simnet"
	"github.com/monaaoua9/dtask/internal/raft/wal"
)

type listFSM struct {
	mu  sync.Mutex
	log []string
}

func (f *listFSM) Apply(_ uint64, data []byte) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, string(data))
	return len(f.log)
}

func (f *listFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(f.log)
}

func (f *listFSM) Restore(b []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = nil
	return json.Unmarshal(b, &f.log)
}

func (f *listFSM) snapshotLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

type member struct {
	id    raft.NodeID
	node  *raft.Node // nil while crashed
	fsm   *listFSM
	store raft.Store
	dir   string
}

type harness struct {
	t         testing.TB
	net       *simnet.Network
	members   map[raft.NodeID]*member
	ids       []raft.NodeID
	snapEvery uint64
	onDisk    bool

	mu      sync.Mutex
	leaders map[uint64]raft.NodeID // term -> leader, for the election safety check

	// background goroutines (leader watcher, chaos writers) go through liveNodes
	rw sync.RWMutex
}

type liveNode struct {
	id raft.NodeID
	n  *raft.Node
}

func (h *harness) liveNodes() []liveNode {
	h.rw.RLock()
	defer h.rw.RUnlock()
	var out []liveNode
	for _, id := range h.ids {
		if m := h.members[id]; m.node != nil {
			out = append(out, liveNode{id, m.node})
		}
	}
	return out
}

type opt func(*harness)

func withSnapshots(n uint64) opt { return func(h *harness) { h.snapEvery = n } }
func withDisk() opt              { return func(h *harness) { h.onDisk = true } }

func newHarness(t testing.TB, n int, opts ...opt) *harness {
	t.Helper()
	h := &harness{
		t:       t,
		net:     simnet.New(time.Now().UnixNano()),
		members: map[raft.NodeID]*member{},
		leaders: map[uint64]raft.NodeID{},
	}
	for _, o := range opts {
		o(h)
	}
	var boot []raft.Server
	for i := 1; i <= n; i++ {
		id := raft.NodeID(fmt.Sprintf("n%d", i))
		h.ids = append(h.ids, id)
		boot = append(boot, raft.Server{ID: id, Addr: string(id), Voter: true})
	}
	for _, id := range h.ids {
		m := &member{id: id, dir: t.TempDir()}
		h.members[id] = m
		h.boot(m, boot)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go h.watchLeaders(stop, done)
	t.Cleanup(func() {
		close(stop)
		<-done
		for _, m := range h.members {
			if m.node != nil {
				m.node.Stop()
			}
		}
	})
	return h
}

func (h *harness) openStore(m *member) raft.Store {
	if !h.onDisk {
		if m.store == nil {
			m.store = raft.NewMemStore()
		}
		return m.store
	}
	s, err := wal.Open(m.dir, wal.Options{NoSync: true})
	require.NoError(h.t, err)
	return s
}

func (h *harness) boot(m *member, bootstrap []raft.Server) {
	m.fsm = &listFSM{}
	n, err := raft.New(raft.Config{
		ID:                 m.id,
		Addr:               string(m.id),
		Bootstrap:          bootstrap,
		Store:              h.openStore(m),
		Transport:          h.net.Endpoint(m.id),
		FSM:                m.fsm,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		HeartbeatInterval:  15 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		RPCTimeout:         100 * time.Millisecond,
		SnapshotThreshold:  h.snapEvery,
	})
	require.NoError(h.t, err)
	h.rw.Lock()
	m.node = n
	h.rw.Unlock()
	h.net.Register(m.id, n)
	n.Start()
}

func (h *harness) crash(id raft.NodeID) {
	m := h.members[id]
	h.net.Unregister(id)
	n := m.node
	h.rw.Lock()
	m.node = nil
	h.rw.Unlock()
	n.Stop()
}

func (h *harness) restart(id raft.NodeID) {
	m := h.members[id]
	require.Nil(h.t, m.node, "restart of a running node")
	h.boot(m, nil)
}

// addFresh starts a brand new empty node that isn't part of any config yet.
func (h *harness) addFresh(id raft.NodeID) *member {
	m := &member{id: id, dir: h.t.TempDir()}
	h.rw.Lock()
	h.members[id] = m
	h.ids = append(h.ids, id)
	h.rw.Unlock()
	h.boot(m, nil)
	return m
}

func (h *harness) running() []*member {
	var out []*member
	for _, id := range h.ids {
		if m := h.members[id]; m.node != nil {
			out = append(out, m)
		}
	}
	return out
}

func (h *harness) watchLeaders(stop, done chan struct{}) {
	defer close(done)
	tk := time.NewTicker(3 * time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tk.C:
		}
		for _, ln := range h.liveNodes() {
			term, ok := ln.n.Leadership()
			if !ok {
				continue
			}
			h.mu.Lock()
			if prev, seen := h.leaders[term]; seen && prev != ln.id {
				h.t.Errorf("election safety violated: term %d has leaders %s and %s", term, prev, ln.id)
			}
			h.leaders[term] = ln.id
			h.mu.Unlock()
		}
	}
}

// leader waits for exactly one running node that thinks it leads, and that a
// majority of running nodes agree with.
func (h *harness) leader(within time.Duration) *member {
	h.t.Helper()
	var found *member
	require.Eventually(h.t, func() bool {
		var leaders []*member
		for _, m := range h.running() {
			if m.node.IsLeader() {
				leaders = append(leaders, m)
			}
		}
		if len(leaders) != 1 {
			return false
		}
		found = leaders[0]
		return true
	}, within, 5*time.Millisecond, "no single leader emerged: %s", h.dump())
	return found
}

func (h *harness) dump() string {
	s := ""
	for _, id := range h.ids {
		m := h.members[id]
		if m.node == nil {
			s += fmt.Sprintf("[%s down] ", id)
			continue
		}
		st := m.node.Status()
		s += fmt.Sprintf("[%s %s t=%d commit=%d applied=%d last=%d leader=%s] ", id, st.Role, st.Term, st.CommitIndex, st.LastApplied, st.LastLogIndex, st.LeaderID)
	}
	return s
}

// propose retries on a fresh leader until the value commits. Only use it for
// values where a duplicate wouldn't matter or can't happen.
func (h *harness) propose(val string) {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var target *member
		for _, m := range h.running() {
			if m.node.IsLeader() {
				target = m
				break
			}
		}
		if target == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := target.node.Propose(ctx, []byte(val))
		cancel()
		if err == nil {
			return
		}
	}
	h.t.Fatalf("could not commit %q: %s", val, h.dump())
}

func (h *harness) converge(within time.Duration, ms ...*member) {
	h.t.Helper()
	if len(ms) == 0 {
		ms = h.running()
	}
	require.Eventually(h.t, func() bool {
		ref := ms[0].fsm.snapshotLog()
		for _, m := range ms[1:] {
			got := m.fsm.snapshotLog()
			if len(got) != len(ref) {
				return false
			}
			for i := range ref {
				if got[i] != ref[i] {
					return false
				}
			}
		}
		return true
	}, within, 10*time.Millisecond, "state machines never converged: %s", h.dump())
}

func ids(ms ...*member) []raft.NodeID {
	var out []raft.NodeID
	for _, m := range ms {
		out = append(out, m.id)
	}
	return out
}

func (h *harness) others(m *member) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range h.ids {
		if id != m.id {
			out = append(out, id)
		}
	}
	return out
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}
