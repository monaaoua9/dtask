// Package scheduler runs the housekeeping that only the leader does:
// requeueing expired leases, garbage-collecting finished tasks and watching
// the other nodes' heartbeats so a dead node's work moves immediately.
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/monaaoua9/dtask/internal/cluster"
	"github.com/monaaoua9/dtask/internal/failure"
	"github.com/monaaoua9/dtask/internal/fsm"
	"github.com/monaaoua9/dtask/internal/raft"
)

type Config struct {
	// how often the loop looks at the world
	Tick time.Duration
	// finished tasks are dropped this long after they finish
	Retention  time.Duration
	GCInterval time.Duration
	// phi at which a node counts as dead. 8 is roughly a 1 in 10^8 false positive rate.
	PhiThreshold float64
	// expected gap between raft heartbeats, seeds the detector
	HeartbeatInterval time.Duration
	Logger            *slog.Logger
}

func (c *Config) setDefaults() {
	if c.Tick == 0 {
		c.Tick = 100 * time.Millisecond
	}
	if c.Retention == 0 {
		c.Retention = time.Hour
	}
	if c.GCInterval == 0 {
		c.GCInterval = 30 * time.Second
	}
	if c.PhiThreshold == 0 {
		c.PhiThreshold = 8
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type Scheduler struct {
	cfg  Config
	raft *raft.Node
	svc  *cluster.Service
	fsm  *fsm.FSM
	det  *failure.Detector

	mu       sync.Mutex
	phi      map[raft.NodeID]float64
	lastSeen map[raft.NodeID]time.Time
	dead     map[raft.NodeID]bool
}

func New(cfg Config, n *raft.Node, svc *cluster.Service, f *fsm.FSM) *Scheduler {
	cfg.setDefaults()
	return &Scheduler{
		cfg: cfg, raft: n, svc: svc, fsm: f,
		det: failure.New(failure.Config{
			BootstrapMean: cfg.HeartbeatInterval,
			MinStdDev:     2 * cfg.HeartbeatInterval, // scheduling jitter shouldn't read as death
		}),
		phi: map[raft.NodeID]float64{}, lastSeen: map[raft.NodeID]time.Time{}, dead: map[raft.NodeID]bool{},
	}
}

// Health is what the status endpoint shows next to each server.
func (s *Scheduler) Health(id raft.NodeID) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	phi := s.phi[id]
	return phi, phi < s.cfg.PhiThreshold
}

func (s *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	var (
		leading bool
		term    uint64
		lastGC  time.Time
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		st := s.raft.Status()
		if st.Role != raft.Leader {
			leading = false
			continue
		}
		if !leading || st.Term != term {
			// fresh reign: forget whatever we believed about peers as a follower
			leading, term = true, st.Term
			s.reset()
			lastGC = time.Now()
		}

		s.watchPeers(ctx, st)

		if next := s.fsm.NextLeaseExpiry(); next != 0 && next <= time.Now().UnixNano() {
			s.reap(ctx, 0)
		}
		if time.Since(lastGC) >= s.cfg.GCInterval {
			lastGC = time.Now()
			s.reap(ctx, s.cfg.Retention)
		}
	}
}

func (s *Scheduler) reset() {
	s.det.Reset()
	s.mu.Lock()
	s.phi = map[raft.NodeID]float64{}
	s.lastSeen = map[raft.NodeID]time.Time{}
	s.dead = map[raft.NodeID]bool{}
	s.mu.Unlock()
}

func (s *Scheduler) reap(ctx context.Context, retention time.Duration) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	r, err := s.svc.Reap(cctx, retention)
	if err != nil {
		if ctx.Err() == nil {
			s.cfg.Logger.Debug("reap failed", "err", err)
		}
		return
	}
	if r.Expired > 0 {
		s.cfg.Logger.Info("requeued expired leases", "count", r.Expired)
	}
}

func (s *Scheduler) watchPeers(ctx context.Context, st raft.Status) {
	now := time.Now()
	for _, p := range st.Peers {
		s.mu.Lock()
		if last := s.lastSeen[p.ID]; !p.LastContact.Equal(last) {
			s.lastSeen[p.ID] = p.LastContact
			s.det.Heartbeat(string(p.ID), p.LastContact)
		}
		phi := s.det.Phi(string(p.ID), now)
		s.phi[p.ID] = phi
		wasDead := s.dead[p.ID]
		isDead := phi >= s.cfg.PhiThreshold
		s.dead[p.ID] = isDead
		s.mu.Unlock()

		if isDead && !wasDead {
			s.expire(ctx, p.ID, phi)
		}
	}
}

func (s *Scheduler) expire(ctx context.Context, id raft.NodeID, phi float64) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	n, err := s.svc.ExpireNode(cctx, id)
	if err != nil {
		s.cfg.Logger.Warn("could not expire dead node", "node", id, "err", err)
		s.mu.Lock()
		s.dead[id] = false // try again next tick
		s.mu.Unlock()
		return
	}
	s.cfg.Logger.Warn("node declared dead, its tasks were requeued", "node", id, "phi", phi, "tasks", n)
}
