package node_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/node"
	"github.com/monaaoua9/dtask/internal/raft"
)

// Whole-system chaos: while tasks flow through the cluster, nodes are killed
// and restarted from disk, links are cut, messages are dropped and delayed.
// Afterwards every task must have finished, no lease token may have been
// issued twice, stored results must come from the winning lease, and all
// replicas must be bit-identical.
func TestChaosCrashesPartitionsAndPacketLoss(t *testing.T) {
	seeds := []int64{11, 22, 33}
	if testing.Short() {
		seeds = seeds[:1]
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runSystemChaos(t, seed, false) })
	}
}

// same run, but every crashed node comes back with a damaged disk tail
func TestChaosTornDiskOnRestart(t *testing.T) {
	seeds := []int64{44, 55}
	if testing.Short() {
		seeds = seeds[:1]
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runSystemChaos(t, seed, true) })
	}
}

func runSystemChaos(t *testing.T, seed int64, tear bool) {
	rng := rand.New(rand.NewSource(seed))
	c := newTestCluster(t, 5, func(cfg *node.Config) {
		cfg.SnapshotThreshold = 80
		cfg.LeaseTTL = 2 * time.Second
	})
	c.leader()

	const total = 250
	var submitted sync.WaitGroup
	submitted.Add(1)
	go func() {
		defer submitted.Done()
		for i := 0; i < total; i++ {
			c.submit(fmt.Sprint("chaos-", i), workSpec{Ms: rng2(seed, i, 80)}, func(r *pb.SubmitRequest) {
				r.Retry = &pb.RetryPolicy{MaxAttempts: 100, BaseDelayMs: 20, MaxDelayMs: 200}
			})
			time.Sleep(time.Duration(10+i%20) * time.Millisecond)
		}
	}()

	down := map[raft.NodeID]bool{}
	deadline := time.Now().Add(14 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(150+rng.Intn(300)) * time.Millisecond)
		id := c.ids[rng.Intn(len(c.ids))]
		act := rng.Intn(6)
		t.Logf("%s chaos action %d on %s (down=%v)", time.Now().Format("05.000"), act, id, down)
		switch act {
		case 0:
			if len(down) < 2 && !down[id] { // never lose the majority
				c.crash(id)
				down[id] = true
			}
		case 1:
			for d := range down {
				if tear {
					tearDisk(t, c.cfg[d].DataDir, rng)
				}
				c.restart(d)
				delete(down, d)
				break
			}
		case 2:
			if !down[id] {
				c.net.Heal()
				c.isolate(id)
			}
		case 3:
			c.net.Heal()
			c.net.SetDropRate(0)
			c.net.SetLatency(0, 0)
		case 4:
			c.net.SetDropRate(0.1)
		case 5:
			c.net.SetLatency(0, 15*time.Millisecond)
		}
	}

	// peace
	c.net.Heal()
	c.net.SetDropRate(0)
	c.net.SetLatency(0, 0)
	for d := range down {
		if tear {
			tearDisk(t, c.cfg[d].DataDir, rng)
		}
		c.restart(d)
	}
	submitted.Wait()
	c.leader()
	c.waitFinished(total, 90*time.Second)

	tasks := c.allTasks()
	require.Len(t, tasks, total, "tasks went missing")
	byToken := map[uint64]execution{}
	for _, e := range c.rec.all() {
		byToken[e.Token] = e
	}
	for _, task := range tasks {
		require.Equal(t, pb.TaskState_TASK_STATE_SUCCEEDED, task.State, "task %s: %s", task.Id, task.LastError)
		winner, ok := byToken[task.LeaseToken]
		require.True(t, ok, "task %s finished under token %d that no handler ever ran with", task.Id, task.LeaseToken)
		require.Equal(t, "done:"+string(winner.Node), string(task.Result))
	}
	tokensUnique(t, c.rec.all())
	c.assertConverged(15 * time.Second)

	extra := len(c.rec.all()) - total
	t.Logf("seed=%d: %d executions for %d tasks (%d re-runs after interruptions)", seed, len(c.rec.all()), total, extra)
}

// what a power cut can leave behind: a record cut off halfway, a tail of
// zeros (file extended, data never landed), noise, or temp files from an
// interrupted atomic replace. Nothing already on disk is touched.
func tearDisk(t *testing.T, dataDir string, rng *rand.Rand) {
	t.Helper()
	dir := filepath.Join(dataDir, "raft")
	junk := func(n int) []byte {
		b := make([]byte, n)
		rng.Read(b)
		return b
	}

	var tail []byte
	switch rng.Intn(4) {
	case 0: // header promising 200 bytes, only a few arrived
		tail = append([]byte{200, 0, 0, 0}, junk(4+rng.Intn(60))...)
	case 1:
		tail = make([]byte, 1+rng.Intn(200))
	case 2:
		tail = junk(1 + rng.Intn(200))
	case 3: // complete-looking header, wrong checksum, payload cut short
		tail = append([]byte{40, 0, 0, 0}, junk(4)...)
		tail = append(tail, junk(10)...)
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Write(tail)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	if rng.Intn(2) == 0 {
		for _, name := range []string{"state.json.tmp", "snapshot.bin.tmp", "wal.log.tmp"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), junk(30), 0o644))
		}
	}
}

func rng2(seed int64, i, max int) int {
	return rand.New(rand.NewSource(seed*1000 + int64(i))).Intn(max)
}
