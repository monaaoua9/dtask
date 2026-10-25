package failure

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func beat(d *Detector, id string, start time.Time, every time.Duration, n int) time.Time {
	t := start
	for i := 0; i < n; i++ {
		d.Heartbeat(id, t)
		t = t.Add(every)
	}
	return t.Add(-every) // time of the last heartbeat
}

func TestPhiGrowsWithSilence(t *testing.T) {
	d := New(Config{})
	t0 := time.Unix(1000, 0)
	last := beat(d, "a", t0, 100*time.Millisecond, 50)

	onTime := d.Phi("a", last.Add(100*time.Millisecond))
	late := d.Phi("a", last.Add(300*time.Millisecond))
	dead := d.Phi("a", last.Add(2*time.Second))
	require.Less(t, onTime, 1.0)
	require.Greater(t, late, onTime)
	require.Greater(t, dead, 8.0, "a node silent for 20 intervals must be well past any sane threshold")
}

func TestUnknownPeerIsNotSuspicious(t *testing.T) {
	require.Equal(t, 0.0, New(Config{}).Phi("ghost", time.Now()))
}

func TestJitteryPeerToleratesMoreDelay(t *testing.T) {
	steady, jittery := New(Config{}), New(Config{})
	t0 := time.Unix(1000, 0)
	var tl time.Time
	tl = beat(steady, "p", t0, 100*time.Millisecond, 60)

	cur := t0
	for i := 0; i < 60; i++ {
		jittery.Heartbeat("p", cur)
		gap := 50 * time.Millisecond
		if i%2 == 0 {
			gap = 250 * time.Millisecond
		}
		cur = cur.Add(gap)
		tl = cur
	}
	_ = tl

	probe := 400 * time.Millisecond
	ps := steady.Phi("p", t0.Add(59*100*time.Millisecond).Add(probe))
	pj := jittery.Phi("p", cur.Add(-250*time.Millisecond).Add(probe))
	require.Greater(t, ps, pj)
}

func TestBootstrapEstimateBeforeEnoughSamples(t *testing.T) {
	d := New(Config{BootstrapMean: 100 * time.Millisecond})
	t0 := time.Unix(1000, 0)
	d.Heartbeat("a", t0)
	require.Greater(t, d.Phi("a", t0.Add(3*time.Second)), 8.0)
}

func TestForgetAndReset(t *testing.T) {
	d := New(Config{})
	t0 := time.Unix(1000, 0)
	d.Heartbeat("a", t0)
	d.Heartbeat("b", t0)
	d.Forget("a")
	require.Equal(t, 0.0, d.Phi("a", t0.Add(time.Hour)))
	require.Greater(t, d.Phi("b", t0.Add(time.Hour)), 0.0)
	d.Reset()
	require.Equal(t, 0.0, d.Phi("b", t0.Add(time.Hour)))
}
