package backoff

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDelayStaysUnderCeilingAndSpreads(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	r := NewRand(1)
	for attempt, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond} {
		var lo, hi time.Duration = time.Hour, 0
		for i := 0; i < 2000; i++ {
			d := p.Delay(attempt, r)
			require.Less(t, d, want)
			if d < lo {
				lo = d
			}
			if d > hi {
				hi = d
			}
		}
		// full jitter really does cover the range, not a thin slice of it
		require.Less(t, lo, want/10)
		require.Greater(t, hi, want*9/10)
	}
}

func TestDelayIsCappedAndNeverOverflows(t *testing.T) {
	p := Policy{Base: time.Second, Max: 30 * time.Second}
	r := NewRand(2)
	for _, attempt := range []int{10, 62, 63, 64, 1000, 1 << 30} {
		d := p.Delay(attempt, r)
		require.GreaterOrEqual(t, d, time.Duration(0))
		require.Less(t, d, 30*time.Second)
	}
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), Policy{Base: time.Millisecond, Max: 2 * time.Millisecond}, NewRand(1), 5, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("flaky")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

func TestRetryGivesUpAndHonoursPermanent(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	err := Retry(context.Background(), Policy{Base: time.Millisecond, Max: time.Millisecond}, NewRand(1), 3, func(context.Context) error {
		calls++
		return boom
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 3, calls)

	calls = 0
	err = Retry(context.Background(), Policy{Base: time.Millisecond, Max: time.Millisecond}, NewRand(1), 10, func(context.Context) error {
		calls++
		return &Permanent{Err: boom}
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, calls)
}

func TestRetryStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := Retry(ctx, Policy{Base: time.Second, Max: time.Second}, NewRand(1), 0, func(context.Context) error {
		return errors.New("nope")
	})
	require.Error(t, err)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}
