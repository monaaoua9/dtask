// Package failure implements a phi accrual failure detector
// (Hayashibara et al.). Instead of a binary alive/dead verdict it reports a
// suspicion level that grows the longer a heartbeat is overdue, relative to
// how regular that peer's heartbeats have been.
package failure

import (
	"math"
	"sync"
	"time"
)

type Config struct {
	// how many inter-arrival samples to keep per peer
	WindowSize int
	// floor for the standard deviation, keeps a perfectly regular peer from
	// being declared dead by a single late packet
	MinStdDev time.Duration
	// what we assume about a peer until we have enough samples
	BootstrapMean time.Duration
	MinSamples    int
}

func (c *Config) setDefaults() {
	if c.WindowSize == 0 {
		c.WindowSize = 100
	}
	if c.MinStdDev == 0 {
		c.MinStdDev = 20 * time.Millisecond
	}
	if c.BootstrapMean == 0 {
		c.BootstrapMean = 100 * time.Millisecond
	}
	if c.MinSamples == 0 {
		c.MinSamples = 3
	}
}

type history struct {
	last      time.Time
	intervals []float64 // ns, ring buffer
	next      int
	full      bool
}

type Detector struct {
	cfg   Config
	mu    sync.Mutex
	peers map[string]*history
}

func New(cfg Config) *Detector {
	cfg.setDefaults()
	return &Detector{cfg: cfg, peers: map[string]*history{}}
}

// Heartbeat records that peer id was heard from at the given time.
func (d *Detector) Heartbeat(id string, at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h := d.peers[id]
	if h == nil {
		d.peers[id] = &history{last: at, intervals: make([]float64, 0, d.cfg.WindowSize)}
		return
	}
	if !at.After(h.last) {
		return
	}
	iv := float64(at.Sub(h.last))
	h.last = at
	if len(h.intervals) < d.cfg.WindowSize {
		h.intervals = append(h.intervals, iv)
		return
	}
	h.intervals[h.next] = iv
	h.next = (h.next + 1) % d.cfg.WindowSize
	h.full = true
}

func (d *Detector) Forget(id string) {
	d.mu.Lock()
	delete(d.peers, id)
	d.mu.Unlock()
}

func (d *Detector) Reset() {
	d.mu.Lock()
	d.peers = map[string]*history{}
	d.mu.Unlock()
}

// Phi returns the suspicion level for id at time now. A phi of 1 means a
// ~10% chance the peer is actually fine, 2 is 1%, 8 is one in 10^8. Unknown
// peers get 0.
func (d *Detector) Phi(id string, now time.Time) float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	h := d.peers[id]
	if h == nil {
		return 0
	}
	mean, std := float64(d.cfg.BootstrapMean), float64(d.cfg.BootstrapMean)/4
	if len(h.intervals) >= d.cfg.MinSamples {
		mean, std = meanStd(h.intervals)
	}
	if min := float64(d.cfg.MinStdDev); std < min {
		std = min
	}
	return phi(float64(now.Sub(h.last)), mean, std)
}

func meanStd(xs []float64) (float64, float64) {
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	var sq float64
	for _, x := range xs {
		sq += (x - mean) * (x - mean)
	}
	return mean, math.Sqrt(sq / float64(len(xs)))
}

// logistic approximation of the normal CDF tail, same one Akka and Cassandra use
func phi(elapsed, mean, std float64) float64 {
	y := (elapsed - mean) / std
	e := math.Exp(-y * (1.5976 + 0.070566*y*y))
	var p float64
	if elapsed > mean {
		p = e / (1 + e)
	} else {
		p = 1 - 1/(1+e)
	}
	if p < 1e-300 {
		return 300
	}
	return -math.Log10(p)
}
