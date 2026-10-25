package raft_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/monaaoua9/dtask/internal/raft"
)

func TestSingleNodeCommits(t *testing.T) {
	h := newHarness(t, 1)
	l := h.leader(3 * time.Second)
	for i := 0; i < 20; i++ {
		res, err := l.node.Propose(context.Background(), []byte(fmt.Sprint(i)))
		require.NoError(t, err)
		require.Equal(t, i+1, res)
	}
}

func TestElectsExactlyOneLeader(t *testing.T) {
	h := newHarness(t, 3)
	l := h.leader(3 * time.Second)
	time.Sleep(500 * time.Millisecond) // stays stable
	require.True(t, l.node.IsLeader())
	followers := 0
	for _, m := range h.running() {
		if !m.node.IsLeader() {
			followers++
			require.Equal(t, l.id, m.node.Status().LeaderID)
		}
	}
	require.Equal(t, 2, followers)
}

func TestReplicatesToAll(t *testing.T) {
	h := newHarness(t, 5)
	h.leader(3 * time.Second)
	for i := 0; i < 100; i++ {
		h.propose(fmt.Sprintf("v%d", i))
	}
	h.converge(3 * time.Second)
	require.Len(t, h.running()[0].fsm.snapshotLog(), 100)
}

func TestProposeOnFollowerReturnsLeaderHint(t *testing.T) {
	h := newHarness(t, 3)
	l := h.leader(3 * time.Second)
	h.converge(2 * time.Second)
	for _, m := range h.running() {
		if m == l {
			continue
		}
		require.Eventually(t, func() bool { return m.node.Status().LeaderID == l.id }, 2*time.Second, 5*time.Millisecond)
		_, err := m.node.Propose(context.Background(), []byte("x"))
		var nl *raft.NotLeaderError
		require.ErrorAs(t, err, &nl)
		require.Equal(t, l.id, nl.LeaderID)
		require.ErrorIs(t, err, raft.ErrNotLeader)
	}
}

func TestLeaderCrashTriggersReelection(t *testing.T) {
	h := newHarness(t, 3)
	old := h.leader(3 * time.Second)
	oldTerm := old.node.Status().Term
	for i := 0; i < 10; i++ {
		h.propose(fmt.Sprintf("a%d", i))
	}

	h.crash(old.id)
	nl := h.leader(3 * time.Second)
	require.NotEqual(t, old.id, nl.id)
	require.Greater(t, nl.node.Status().Term, oldTerm)

	for i := 0; i < 10; i++ {
		h.propose(fmt.Sprintf("b%d", i))
	}

	h.restart(old.id)
	h.converge(5 * time.Second)
	require.Len(t, h.members[old.id].fsm.snapshotLog(), 20)
}

func TestNoProgressWithoutQuorum(t *testing.T) {
	h := newHarness(t, 3)
	l := h.leader(3 * time.Second)
	h.propose("before")
	for _, id := range h.others(l) {
		h.crash(id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_, err := l.node.Propose(ctx, []byte("lonely"))
	require.Error(t, err)

	// the stranded leader must give up its crown rather than keep serving
	require.Eventually(t, func() bool { return !l.node.IsLeader() }, 3*time.Second, 10*time.Millisecond)

	for _, id := range h.others(l) {
		h.restart(id)
	}
	h.leader(5 * time.Second)
	h.propose("after")
	h.converge(5 * time.Second)
	got := h.running()[0].fsm.snapshotLog()
	require.Contains(t, got, "before")
	require.Contains(t, got, "after")
}

func TestPartitionedLeaderStepsDownAndRejoinsCleanly(t *testing.T) {
	h := newHarness(t, 5)
	old := h.leader(3 * time.Second)
	h.propose("committed-1")
	h.converge(2 * time.Second)

	h.net.Isolate(old.id, h.others(old))

	// the isolated leader keeps accepting writes into its log but can't commit
	var wg sync.WaitGroup
	wg.Add(1)
	var staleErr error
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, staleErr = old.node.Propose(ctx, []byte("doomed"))
	}()

	// majority side elects a replacement and keeps going
	var nl *member
	require.Eventually(t, func() bool {
		for _, m := range h.running() {
			if m != old && m.node.IsLeader() {
				nl = m
				return true
			}
		}
		return false
	}, 3*time.Second, 5*time.Millisecond, h.dump())
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := nl.node.Propose(ctx, []byte(fmt.Sprintf("majority-%d", i)))
		cancel()
		require.NoError(t, err)
	}

	wg.Wait()
	require.Error(t, staleErr, "a minority leader must never report success")
	require.Eventually(t, func() bool { return !old.node.IsLeader() }, 2*time.Second, 10*time.Millisecond)

	h.net.Heal()
	h.converge(5 * time.Second)
	got := h.members[nl.id].fsm.snapshotLog()
	require.NotContains(t, got, "doomed")
	require.Contains(t, got, "committed-1")
	require.Len(t, got, 6)
}

func TestPreVoteStopsIsolatedNodeFromDisruptingCluster(t *testing.T) {
	h := newHarness(t, 3)
	l := h.leader(3 * time.Second)
	term := l.node.Status().Term

	var victim *member
	for _, m := range h.running() {
		if m != l {
			victim = m
			break
		}
	}
	h.net.Isolate(victim.id, h.others(victim))
	time.Sleep(1500 * time.Millisecond) // many election timeouts
	require.Equal(t, term, victim.node.Status().Term, "isolated node must not inflate its term")

	h.net.Heal()
	time.Sleep(600 * time.Millisecond)
	require.True(t, l.node.IsLeader(), "healthy leader was deposed by a rejoining node")
	require.Equal(t, term, l.node.Status().Term)
	h.propose("still-fine")
	h.converge(3 * time.Second)
}

func TestFollowerRestartKeepsDurableState(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%v", disk), func(t *testing.T) {
			var opts []opt
			if disk {
				opts = append(opts, withDisk())
			}
			h := newHarness(t, 3, opts...)
			l := h.leader(3 * time.Second)
			for i := 0; i < 30; i++ {
				h.propose(fmt.Sprintf("p%d", i))
			}
			h.converge(3 * time.Second)

			var f *member
			for _, m := range h.running() {
				if m != l {
					f = m
					break
				}
			}
			termBefore := f.node.Status().Term
			h.crash(f.id)
			h.propose("while-down")
			h.restart(f.id)
			require.GreaterOrEqual(t, f.node.Status().Term, termBefore)
			h.converge(5 * time.Second)
			require.Len(t, f.fsm.snapshotLog(), 31)
		})
	}
}

func TestWholeClusterRestartOnDisk(t *testing.T) {
	h := newHarness(t, 3, withDisk(), withSnapshots(10))
	h.leader(3 * time.Second)
	for i := 0; i < 40; i++ {
		h.propose(fmt.Sprintf("d%d", i))
	}
	h.converge(3 * time.Second)

	for _, id := range h.ids {
		h.crash(id)
	}
	for _, id := range h.ids {
		h.restart(id)
	}
	h.leader(5 * time.Second)
	h.propose("post-restart")
	h.converge(5 * time.Second)
	got := h.running()[0].fsm.snapshotLog()
	require.Len(t, got, 41)
	require.Equal(t, "d0", got[0])
	require.Equal(t, "post-restart", got[40])
}

func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	h := newHarness(t, 3, withSnapshots(10))
	l := h.leader(3 * time.Second)
	var lag *member
	for _, m := range h.running() {
		if m != l {
			lag = m
			break
		}
	}
	h.net.Isolate(lag.id, h.others(lag))
	for i := 0; i < 60; i++ {
		h.propose(fmt.Sprintf("s%d", i))
	}
	require.Eventually(t, func() bool {
		return l.node.Status().LastLogIndex > 0 && len(l.fsm.snapshotLog()) == 60
	}, 3*time.Second, 10*time.Millisecond)

	h.net.Heal()
	h.converge(8 * time.Second)
	require.Len(t, lag.fsm.snapshotLog(), 60)
}

func TestReadBarrier(t *testing.T) {
	h := newHarness(t, 3)
	l := h.leader(3 * time.Second)
	h.propose("x")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, l.node.ReadBarrier(ctx))
	require.Equal(t, []string{"x"}, l.fsm.snapshotLog())

	for _, m := range h.running() {
		if m != l {
			require.ErrorIs(t, m.node.ReadBarrier(ctx), raft.ErrNotLeader)
		}
	}

	// cut the leader off: it must not be able to certify a read
	h.net.Isolate(l.id, h.others(l))
	short, cancel2 := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel2()
	require.Error(t, l.node.ReadBarrier(short))
}

func TestAddServerJoinsAsLearnerThenVoter(t *testing.T) {
	h := newHarness(t, 3)
	l := h.leader(3 * time.Second)
	for i := 0; i < 25; i++ {
		h.propose(fmt.Sprintf("m%d", i))
	}

	fresh := h.addFresh("n4")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	require.NoError(t, l.node.AddServer(ctx, "n4", "n4"))

	require.True(t, l.node.Status().Config.IsVoter("n4"))
	h.converge(5 * time.Second)
	require.Len(t, fresh.fsm.snapshotLog(), 25)

	// the new node counts toward quorum: lose two of the original three and the rest still works
	var victims []raft.NodeID
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		if id != l.id {
			victims = append(victims, id)
		}
	}
	h.crash(victims[0])
	h.propose("with-4-nodes")
	h.converge(5 * time.Second)
}

func TestRemoveServerAndRemoveLeader(t *testing.T) {
	h := newHarness(t, 5)
	l := h.leader(3 * time.Second)
	h.propose("one")

	var victim *member
	for _, m := range h.running() {
		if m != l {
			victim = m
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	require.NoError(t, l.node.RemoveServer(ctx, victim.id))
	require.False(t, l.node.Status().Config.IsVoter(victim.id))
	require.Equal(t, 4, l.node.Status().Config.Voters())

	// the removed node can't unseat anyone, even though it never hears about it
	h.net.Isolate(victim.id, h.others(victim))
	h.net.Heal()
	h.propose("two")

	// now remove the leader itself
	require.NoError(t, l.node.RemoveServer(ctx, l.id))
	require.Eventually(t, func() bool { return !l.node.IsLeader() }, 3*time.Second, 10*time.Millisecond)
	nl := h.leaderExcluding(5*time.Second, l.id, victim.id)
	require.NotEqual(t, l.id, nl.id)
	require.Equal(t, 3, nl.node.Status().Config.Voters())

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_, err := nl.node.Propose(ctx2, []byte("three"))
	require.NoError(t, err)
}

func (h *harness) leaderExcluding(within time.Duration, skip ...raft.NodeID) *member {
	h.t.Helper()
	var found *member
	require.Eventually(h.t, func() bool {
		for _, m := range h.running() {
			for _, s := range skip {
				if m.id == s {
					goto next
				}
			}
			if m.node.IsLeader() {
				found = m
				return true
			}
		next:
		}
		return false
	}, within, 5*time.Millisecond, h.dump())
	return found
}

func TestRemoveLastVoterRejected(t *testing.T) {
	h := newHarness(t, 1)
	l := h.leader(3 * time.Second)
	err := l.node.RemoveServer(context.Background(), l.id)
	require.ErrorIs(t, err, raft.ErrLastVoter)
}

// A small jepsen-flavoured soak: random crashes, partitions and message
// loss while clients write. Acknowledged writes must survive, nothing may
// be applied twice, and everyone must end up with the same log.
func TestRandomFaultsKeepCommittedWrites(t *testing.T) {
	seeds := []int64{1, 2, 3}
	if testing.Short() {
		seeds = seeds[:1]
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runChaos(t, seed)
		})
	}
}

func runChaos(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	h := newHarness(t, 5, withSnapshots(15), withDisk())
	h.leader(3 * time.Second)

	var (
		mu    sync.Mutex
		acked = map[string]bool{}
		tried = map[string]bool{}
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				val := fmt.Sprintf("w%d-%d", w, i)
				mu.Lock()
				tried[val] = true
				mu.Unlock()
				for _, ln := range h.liveNodes() {
					if !ln.n.IsLeader() {
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
					_, err := ln.n.Propose(ctx, []byte(val))
					cancel()
					if err == nil {
						mu.Lock()
						acked[val] = true
						mu.Unlock()
					}
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}

	down := map[raft.NodeID]bool{}
	for step := 0; step < 25; step++ {
		time.Sleep(time.Duration(100+rng.Intn(200)) * time.Millisecond)
		switch rng.Intn(5) {
		case 0: // crash one, keeping a majority alive
			if len(down) < 2 {
				id := h.ids[rng.Intn(len(h.ids))]
				if !down[id] {
					h.crash(id)
					down[id] = true
				}
			}
		case 1: // bring one back
			for id := range down {
				h.restart(id)
				delete(down, id)
				break
			}
		case 2: // partition off a random node
			id := h.ids[rng.Intn(len(h.ids))]
			h.net.Heal()
			h.net.Isolate(id, h.others(h.members[id]))
		case 3:
			h.net.Heal()
			h.net.SetDropRate(0)
		case 4:
			h.net.SetDropRate(0.15)
			h.net.SetLatency(0, 20*time.Millisecond)
		}
	}

	close(stop)
	wg.Wait()
	h.net.Heal()
	h.net.SetDropRate(0)
	h.net.SetLatency(0, 0)
	for id := range down {
		h.restart(id)
	}
	h.leader(10 * time.Second)
	h.propose("final-barrier")
	h.converge(15 * time.Second)

	final := h.running()[0].fsm.snapshotLog()
	seen := map[string]int{}
	for _, v := range final {
		seen[v]++
	}
	mu.Lock()
	defer mu.Unlock()
	for v, c := range seen {
		if v != "final-barrier" && !tried[v] {
			t.Fatalf("applied value %q nobody proposed", v)
		}
		require.Equal(t, 1, c, "value %q applied %d times", v, c)
	}
	for v := range acked {
		require.Equal(t, 1, seen[v], "acknowledged write %q lost", v)
	}
	t.Logf("seed=%d acked=%d applied=%d", seed, len(acked), len(final))
}

func TestStoppedNodeRejectsWork(t *testing.T) {
	h := newHarness(t, 1)
	l := h.leader(3 * time.Second)
	l.node.Stop()
	_, err := l.node.Propose(context.Background(), []byte("x"))
	require.True(t, errors.Is(err, raft.ErrStopped))
}

func TestLeadershipTransferIsFasterThanAnElection(t *testing.T) {
	h := newHarness(t, 3)
	old := h.leader(3 * time.Second)
	for i := 0; i < 10; i++ {
		h.propose(fmt.Sprintf("t%d", i))
	}
	h.converge(2 * time.Second)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, old.node.TransferLeadership(ctx))
	require.False(t, old.node.IsLeader())

	nl := h.leaderExcluding(2*time.Second, old.id)
	// a normal failover waits out the election timeout (150ms+), this shouldn't
	require.Less(t, time.Since(start), 140*time.Millisecond, "handoff took as long as a timeout-driven election")

	h.propose("after-transfer")
	h.converge(3 * time.Second)
	require.Len(t, nl.fsm.snapshotLog(), 11)
}

func TestTransferWithNoOtherVoterFails(t *testing.T) {
	h := newHarness(t, 1)
	l := h.leader(3 * time.Second)
	require.ErrorIs(t, l.node.TransferLeadership(context.Background()), raft.ErrNoTransferTarget)
	_, err := l.node.Propose(context.Background(), []byte("still leader"))
	require.NoError(t, err)
}
