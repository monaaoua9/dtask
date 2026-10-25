package backoff

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Policy is exponential backoff with full jitter: attempt n waits a random
// time in [0, min(Max, Base*2^n)). Randomising the whole range, not just a
// slice of it, is what keeps a herd of retrying workers from re-syncing.
type Policy struct {
	Base time.Duration
	Max  time.Duration
}

func (p Policy) ceiling(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	c := float64(p.Base) * math.Pow(2, float64(attempt))
	if c > float64(p.Max) || math.IsInf(c, 0) {
		return p.Max
	}
	return time.Duration(c)
}

// Delay picks the wait before retry number attempt (0 = first retry).
func (p Policy) Delay(attempt int, r *Rand) time.Duration {
	c := p.ceiling(attempt)
	if c <= 0 {
		return 0
	}
	return time.Duration(r.Int63n(int64(c)))
}

// Rand is a rand.Rand that can be shared between goroutines.
type Rand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func NewRand(seed int64) *Rand { return &Rand{r: rand.New(rand.NewSource(seed))} }

func (r *Rand) Int63n(n int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.r.Int63n(n)
}

// Permanent wraps an error to tell Retry not to try again.
type Permanent struct{ Err error }

func (p *Permanent) Error() string { return p.Err.Error() }
func (p *Permanent) Unwrap() error { return p.Err }

// Retry calls fn until it succeeds, returns a *Permanent, runs out of
// attempts (0 means unlimited) or ctx ends.
func Retry(ctx context.Context, p Policy, r *Rand, attempts int, fn func(ctx context.Context) error) error {
	var err error
	for i := 0; attempts == 0 || i < attempts; i++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		if perm, ok := err.(*Permanent); ok {
			return perm.Err
		}
		if attempts != 0 && i == attempts-1 {
			break
		}
		t := time.NewTimer(p.Delay(i, r))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return err
		}
	}
	return err
}
