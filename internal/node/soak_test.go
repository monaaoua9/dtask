package node_test

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/monaaoua9/dtask/internal/node"
)

func settledGoroutines() int {
	var n int
	for i := 0; i < 20; i++ {
		runtime.GC()
		n = runtime.NumGoroutine()
		time.Sleep(50 * time.Millisecond)
	}
	return n
}

// a cluster that ran work, lost a node and a leader, then crashed must not
// leave goroutines behind
func TestNoGoroutineLeakAfterShutdown(t *testing.T) {
	before := settledGoroutines()

	func() {
		c := newTestCluster(t, 3)
		c.leader()
		for i := 0; i < 40; i++ {
			c.submit(fmt.Sprint("leak-", i), workSpec{Ms: 5})
		}
		c.waitFinished(40, 20*time.Second)
		old := c.leader()
		c.crash(old.ID())
		c.leader()
		c.restart(old.ID())
		c.assertConverged(15 * time.Second)
		for _, n := range c.alive() {
			c.crash(n.ID())
		}
	}()

	var after int
	require.Eventually(t, func() bool {
		after = settledGoroutines()
		return after <= before+2
	}, 30*time.Second, 100*time.Millisecond, "goroutines before=%d", before)
	t.Logf("goroutines before=%d after=%d", before, after)
}

// DTASK_SOAK=10m go test -run TestSoak ./internal/node
// Keeps the cluster busy with periodic crashes and checks that neither the
// goroutine count nor the live heap keeps growing once it has warmed up.
func TestSoak(t *testing.T) {
	spec := os.Getenv("DTASK_SOAK")
	if spec == "" {
		t.Skip("set DTASK_SOAK=<duration> to run")
	}
	dur, err := time.ParseDuration(spec)
	require.NoError(t, err)

	c := newTestCluster(t, 3, func(cfg *node.Config) {
		cfg.SnapshotThreshold = 500
	})
	c.leader()

	type sample struct {
		gor  int
		heap uint64
		done uint64
	}
	var samples []sample
	submitted := 0
	end := time.Now().Add(dur)
	next := time.Now().Add(time.Minute)
	sampleEvery := dur / 10
	lastSample := time.Now()

	for time.Now().Before(end) {
		for i := 0; i < 20; i++ {
			c.submit("soak-"+strconv.Itoa(submitted), workSpec{Ms: 2})
			submitted++
		}
		time.Sleep(100 * time.Millisecond)

		if time.Now().After(next) {
			next = time.Now().Add(time.Minute)
			l := c.leader()
			c.crash(l.ID())
			c.leader()
			c.restart(l.ID())
		}
		if time.Since(lastSample) > sampleEvery {
			lastSample = time.Now()
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			_, _, _, done, dead := c.counts()
			samples = append(samples, sample{runtime.NumGoroutine(), m.HeapAlloc, done + dead})
			t.Logf("t=%s goroutines=%d heap=%dMB finished=%d submitted=%d",
				time.Since(end.Add(-dur)).Round(time.Second), samples[len(samples)-1].gor, m.HeapAlloc>>20, done+dead, submitted)
		}
	}

	c.waitFinished(submitted, 2*time.Minute)
	c.assertConverged(30 * time.Second)

	require.GreaterOrEqual(t, len(samples), 6)
	mid, last := samples[len(samples)/2], samples[len(samples)-1]
	require.LessOrEqual(t, last.gor, mid.gor+20, "goroutine count keeps growing")
	// finished tasks are retained until GC, so allow some growth with the task count
	require.LessOrEqual(t, last.heap, mid.heap*3+(64<<20), "heap keeps growing")
}
