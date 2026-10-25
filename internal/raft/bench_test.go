package raft_test

import (
	"io"
	"log/slog"

	"context"
	"github.com/monaaoua9/dtask/internal/raft"
	"github.com/monaaoua9/dtask/internal/raft/simnet"
	"github.com/monaaoua9/dtask/internal/raft/wal"
	"testing"
	"time"
)

// 3 nodes on the in-process network with memory stores: this measures the
// protocol and its batching, not the disk
func BenchmarkProposeParallel(b *testing.B) {
	h := newHarness(b, 3)
	l := h.leader(5 * time.Second)
	payload := []byte("0123456789abcdef0123456789abcdef")

	b.ResetTimer()
	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := l.node.Propose(context.Background(), payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
}

// one node, real WAL with fsync: shows what group commit buys
func BenchmarkProposeWAL(b *testing.B) {
	st, err := wal.Open(b.TempDir(), wal.Options{})
	if err != nil {
		b.Fatal(err)
	}
	n, err := raft.New(raft.Config{
		ID: "n1", Addr: "n1", Bootstrap: []raft.Server{{ID: "n1", Addr: "n1", Voter: true}},
		Store: st, Transport: simnet.New(1).Endpoint("n1"), FSM: &nopFSM{},
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		HeartbeatInterval: 10 * time.Millisecond, ElectionTimeoutMin: 50 * time.Millisecond, ElectionTimeoutMax: 100 * time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	n.Start()
	defer n.Stop()
	for !n.IsLeader() {
		time.Sleep(5 * time.Millisecond)
	}

	b.ResetTimer()
	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := n.Propose(context.Background(), []byte("0123456789abcdef")); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
}

type nopFSM struct{}

func (*nopFSM) Apply(uint64, []byte) any  { return nil }
func (*nopFSM) Snapshot() ([]byte, error) { return nil, nil }
func (*nopFSM) Restore([]byte) error      { return nil }
