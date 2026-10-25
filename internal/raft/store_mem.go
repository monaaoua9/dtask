package raft

import (
	"fmt"
	"sync"
)

// MemStore keeps everything in memory. It survives Node restarts as long as
// the same value is reused, which is how the tests simulate a disk.
type MemStore struct {
	mu       sync.Mutex
	state    PersistentState
	entries  []Entry
	snap     *SnapshotMeta
	snapData []byte
}

func NewMemStore() *MemStore { return &MemStore{} }

func (m *MemStore) Load() (PersistentState, []Entry, *SnapshotMeta, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]Entry(nil), m.entries...)
	var meta *SnapshotMeta
	if m.snap != nil {
		c := *m.snap
		c.Configuration = c.Configuration.Clone()
		meta = &c
	}
	return m.state, out, meta, m.snapData, nil
}

func (m *MemStore) SaveState(s PersistentState) error {
	m.mu.Lock()
	m.state = s
	m.mu.Unlock()
	return nil
}

func (m *MemStore) Append(es []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.nextIndexLocked()
	for i, e := range es {
		if e.Index != next+uint64(i) {
			return fmt.Errorf("memstore: append index %d, want %d", e.Index, next+uint64(i))
		}
	}
	m.entries = append(m.entries, es...)
	return nil
}

func (m *MemStore) nextIndexLocked() uint64 {
	if len(m.entries) > 0 {
		return m.entries[len(m.entries)-1].Index + 1
	}
	if m.snap != nil {
		return m.snap.Index + 1
	}
	return 1
}

func (m *MemStore) TruncateSuffix(from uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range m.entries {
		if e.Index >= from {
			m.entries = m.entries[:i]
			return nil
		}
	}
	return nil
}

func (m *MemStore) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta.Configuration = meta.Configuration.Clone()
	m.snap, m.snapData = &meta, append([]byte(nil), data...)
	keep := m.entries[:0:0]
	for _, e := range m.entries {
		if e.Index > meta.Index {
			keep = append(keep, e)
		}
	}
	m.entries = keep
	return nil
}

func (m *MemStore) Close() error { return nil }
