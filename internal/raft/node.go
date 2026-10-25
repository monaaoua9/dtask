// Package raft is a from-scratch implementation of the Raft consensus
// protocol: leader election with pre-vote and leader stickiness, log
// replication, snapshots, single-server membership changes with learner
// catch-up, CheckQuorum and linearizable read barriers.
//
// Locking: Node.mu guards all consensus state. applyMu serialises FSM
// mutation (Apply/Restore/Snapshot) and is always taken before mu, never after.
// Methods ending in Locked expect mu to be held. Nothing blocking (RPC, FSM
// apply) happens with mu held; fsync inside Store calls does.
package raft

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Config struct {
	ID   NodeID
	Addr string
	// Bootstrap seeds the very first membership. Every initial node must be
	// given the same list; nodes joining later leave it empty and get added
	// through AddServer on the leader.
	Bootstrap []Server

	Store     Store
	Transport Transport
	FSM       FSM
	Logger    *slog.Logger
	Metrics   *Metrics

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration

	// SnapshotThreshold is how many applied entries pile up before the log is
	// compacted. 0 turns snapshots off.
	SnapshotThreshold uint64
	MaxAppendEntries  int
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 10 * c.HeartbeatInterval
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = c.ElectionTimeoutMin
	}
	if c.MaxAppendEntries == 0 {
		c.MaxAppendEntries = 256
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type peer struct {
	cancel      context.CancelFunc
	srv         Server
	nextIndex   uint64
	matchIndex  uint64
	lastAck     uint64    // send sequence of the newest request this peer acked
	lastContact time.Time // when we last heard anything back
	trigger     chan struct{}
}

type future struct {
	term  uint64
	start time.Time
	done  chan struct{}
	val   any
	err   error
}

func (f *future) resolve(v any, err error) {
	f.val, f.err = v, err
	close(f.done)
}

type Node struct {
	cfg     Config
	id      NodeID
	log     *slog.Logger
	store   Store
	trans   Transport
	fsm     FSM
	metrics *Metrics

	baseCtx    context.Context
	baseCancel context.CancelFunc

	applyMu sync.Mutex

	mu                sync.Mutex
	role              Role
	term              uint64
	votedFor          NodeID
	leaderID          NodeID
	rlog              raftLog
	conf              Configuration // latest config in the log, committed or not
	confIndex         uint64
	appliedConf       Configuration // config as of lastApplied
	commitIndex       uint64
	lastApplied       uint64
	snapMeta          SnapshotMeta
	snapData          []byte
	electionDeadline  time.Time
	lastLeaderContact time.Time
	campaigning       bool
	rng               *rand.Rand

	leaderCtx    context.Context
	leaderCancel context.CancelFunc
	leaderSince  time.Time
	noopIndex    uint64
	peers        map[NodeID]*peer
	pending      map[uint64]*future
	transferring bool   // refusing proposals while handing leadership away
	sendSeq      uint64 // orders request sends against read barriers; wall clocks are too coarse

	changed chan struct{} // closed and replaced whenever something waiters care about moves
	applyCh chan struct{}
	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup
	confSem chan struct{}
	propCh  chan *proposal
}

func New(cfg Config) (*Node, error) {
	cfg.setDefaults()
	ps, entries, meta, snapData, err := cfg.Store.Load()
	if err != nil {
		return nil, fmt.Errorf("load store: %w", err)
	}

	h := fnv.New64a()
	h.Write([]byte(cfg.ID))
	n := &Node{
		cfg:      cfg,
		id:       cfg.ID,
		log:      cfg.Logger.With("node", string(cfg.ID)),
		store:    cfg.Store,
		trans:    cfg.Transport,
		fsm:      cfg.FSM,
		metrics:  cfg.Metrics,
		term:     ps.Term,
		votedFor: ps.VotedFor,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(h.Sum64()))),
		pending:  map[uint64]*future{},
		changed:  make(chan struct{}),
		applyCh:  make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		confSem:  make(chan struct{}, 1),
		propCh:   make(chan *proposal, 1024),
	}
	n.baseCtx, n.baseCancel = context.WithCancel(context.Background())

	if meta != nil {
		n.rlog.snapIndex, n.rlog.snapTerm = meta.Index, meta.Term
		n.snapMeta, n.snapData = *meta, snapData
		n.appliedConf = meta.Configuration.Clone()
		n.commitIndex, n.lastApplied = meta.Index, meta.Index
		if err := n.fsm.Restore(snapData); err != nil {
			return nil, fmt.Errorf("restore snapshot: %w", err)
		}
	}
	n.rlog.entries = entries

	if len(entries) == 0 && meta == nil && len(cfg.Bootstrap) > 0 {
		conf := Configuration{Servers: append([]Server(nil), cfg.Bootstrap...)}
		sort.Slice(conf.Servers, func(i, j int) bool { return conf.Servers[i].ID < conf.Servers[j].ID })
		e := Entry{Index: 1, Term: 1, Type: EntryConfig, Data: EncodeConfiguration(conf)}
		if err := n.store.Append([]Entry{e}); err != nil {
			return nil, err
		}
		n.rlog.append(e)
		if n.term < 1 {
			n.term = 1
			if err := n.store.SaveState(PersistentState{Term: n.term, VotedFor: n.votedFor}); err != nil {
				return nil, err
			}
		}
	}

	n.recomputeConfigLocked()
	n.resetElectionTimerLocked()
	n.lastLeaderContact = time.Time{}
	return n, nil
}

func (n *Node) Start() {
	n.wg.Add(3)
	go n.tickLoop()
	go n.applyLoop()
	go n.proposeLoop()
	n.signalApply()
}

func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.baseCancel()
	if n.role == Leader {
		n.stopLeaderLocked(ErrStopped)
	}
	n.mu.Unlock()
	n.wg.Wait()
	_ = n.store.Close()
}

func (n *Node) must(err error) {
	if err != nil {
		// carrying on after a failed write would break Raft's safety rules
		n.log.Error("fatal storage error", "err", err)
		panic(fmt.Sprintf("raft %s: storage failure: %v", n.id, err))
	}
}

func (n *Node) persistStateLocked() {
	n.must(n.store.SaveState(PersistentState{Term: n.term, VotedFor: n.votedFor}))
}

func (n *Node) appendEntryLocked(t EntryType, data []byte) uint64 {
	e := Entry{Index: n.rlog.lastIndex() + 1, Term: n.term, Type: t, Data: data}
	n.must(n.store.Append([]Entry{e}))
	n.rlog.append(e)
	return e.Index
}

func (n *Node) notifyLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) resetElectionTimerLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	d := n.cfg.ElectionTimeoutMin
	if span > 0 {
		d += time.Duration(n.rng.Int63n(int64(span)))
	}
	n.electionDeadline = time.Now().Add(d)
}

func (n *Node) recomputeConfigLocked() {
	for i := len(n.rlog.entries) - 1; i >= 0; i-- {
		e := n.rlog.entries[i]
		if e.Type != EntryConfig {
			continue
		}
		c, err := DecodeConfiguration(e.Data)
		n.must(err)
		n.conf, n.confIndex = c, e.Index
		return
	}
	n.conf, n.confIndex = n.snapMeta.Configuration.Clone(), n.snapMeta.Index
}

func (n *Node) becomeFollowerLocked(term uint64, leader NodeID) {
	wasLeader := n.role == Leader
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.persistStateLocked()
		n.leaderID = ""
	}
	n.role = Follower
	if leader != "" {
		n.leaderID = leader
	}
	if wasLeader {
		n.leaderID = leader
		n.stopLeaderLocked(ErrLeadershipLost)
	}
	n.resetElectionTimerLocked()
	n.updateMetricsLocked()
	n.notifyLocked()
}

func (n *Node) becomeLeaderLocked() {
	now := time.Now()
	n.role = Leader
	n.leaderID = n.id
	n.leaderCtx, n.leaderCancel = context.WithCancel(n.baseCtx)
	n.leaderSince = now
	n.peers = map[NodeID]*peer{}
	last := n.rlog.lastIndex()
	for _, s := range n.conf.Servers {
		if s.ID != n.id {
			n.startPeerLocked(s, last)
		}
	}
	// a leader may only count replicas for entries of its own term, so commit
	// a no-op first; it also drags older entries over the line
	n.noopIndex = n.appendEntryLocked(EntryNoop, nil)
	n.advanceCommitLocked()
	n.kickReplicatorsLocked()
	n.metrics.becameLeader()
	n.updateMetricsLocked()
	n.notifyLocked()
	n.log.Info("became leader", "term", n.term, "last_index", n.rlog.lastIndex())
}

func (n *Node) stopLeaderLocked(reason error) {
	n.transferring = false
	if n.leaderCancel != nil {
		n.leaderCancel()
		n.leaderCancel = nil
	}
	n.peers = nil
	for idx, f := range n.pending {
		f.resolve(nil, reason)
		delete(n.pending, idx)
		n.metrics.proposal("lost")
	}
}

func (n *Node) updateMetricsLocked() {
	n.metrics.setState(n.term, n.role, n.commitIndex, n.lastApplied, n.rlog.lastIndex())
}

func (n *Node) tickLoop() {
	defer n.wg.Done()
	tick := n.cfg.HeartbeatInterval / 4
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.onTick()
		}
	}
}

func (n *Node) onTick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	now := time.Now()
	if n.role == Leader {
		n.checkQuorumLocked(now)
		return
	}
	if now.Before(n.electionDeadline) || n.campaigning || !n.conf.IsVoter(n.id) {
		return
	}
	n.resetElectionTimerLocked()
	n.leaderID = "" // timed out, so whoever we thought was leader isn't
	n.campaigning = true
	n.wg.Add(1)
	go n.campaign(false)
}

// checkQuorumLocked makes a leader that can't reach a majority step down
// instead of sitting on a minority island accepting writes it can never commit.
func (n *Node) checkQuorumLocked(now time.Time) {
	if now.Sub(n.leaderSince) < n.cfg.ElectionTimeoutMax {
		return
	}
	active := 0
	if n.conf.IsVoter(n.id) {
		active++
	}
	for _, s := range n.conf.Servers {
		if s.ID == n.id || !s.Voter {
			continue
		}
		if p := n.peers[s.ID]; p != nil && now.Sub(p.lastContact) < n.cfg.ElectionTimeoutMax {
			active++
		}
	}
	if active >= n.conf.Quorum() {
		return
	}
	n.log.Warn("lost contact with quorum, stepping down", "term", n.term, "active", active)
	n.metrics.stepDownNoQuorum()
	n.becomeFollowerLocked(n.term, "")
}

func (n *Node) logUpToDateLocked(lastIndex, lastTerm uint64) bool {
	myTerm, myIndex := n.rlog.lastTerm(), n.rlog.lastIndex()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

// campaign runs one election attempt. A transfer skips the pre-vote round
// and tells voters not to apply leader stickiness: the old leader asked for this.
func (n *Node) campaign(transfer bool) {
	defer n.wg.Done()
	defer func() {
		n.mu.Lock()
		n.campaigning = false
		n.mu.Unlock()
	}()

	n.mu.Lock()
	if n.stopped || n.role == Leader || !n.conf.IsVoter(n.id) {
		n.mu.Unlock()
		return
	}
	term := n.term
	conf := n.conf.Clone()
	pre := &RequestVoteRequest{
		Term: term + 1, CandidateID: n.id,
		LastLogIndex: n.rlog.lastIndex(), LastLogTerm: n.rlog.lastTerm(), PreVote: true,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.baseCtx, n.cfg.ElectionTimeoutMax)
	defer cancel()

	if !transfer {
		if !n.gatherVotes(ctx, conf, pre) {
			n.metrics.election("pre", "lost")
			return
		}
		n.metrics.election("pre", "won")
	}

	n.mu.Lock()
	// somebody may have shown up as leader while we were probing
	if n.stopped || n.term != term || n.role == Leader || (!transfer && time.Since(n.lastLeaderContact) < n.cfg.ElectionTimeoutMin) {
		n.mu.Unlock()
		return
	}
	n.term++
	n.role = Candidate
	n.votedFor = n.id
	n.leaderID = ""
	n.persistStateLocked()
	n.resetElectionTimerLocked()
	n.updateMetricsLocked()
	req := &RequestVoteRequest{
		Term: n.term, CandidateID: n.id,
		LastLogIndex: n.rlog.lastIndex(), LastLogTerm: n.rlog.lastTerm(),
		LeadershipTransfer: transfer,
	}
	conf = n.conf.Clone()
	n.mu.Unlock()

	won := n.gatherVotes(ctx, conf, req)

	n.mu.Lock()
	defer n.mu.Unlock()
	if !won || n.stopped || n.term != req.Term || n.role != Candidate {
		n.metrics.election("vote", "lost")
		return
	}
	n.metrics.election("vote", "won")
	n.becomeLeaderLocked()
}

func (n *Node) gatherVotes(ctx context.Context, conf Configuration, req *RequestVoteRequest) bool {
	need := conf.Quorum()
	votes := 0
	if conf.IsVoter(n.id) {
		votes = 1
	}
	if votes >= need {
		return true
	}

	type result struct {
		resp *RequestVoteResponse
		err  error
	}
	ch := make(chan result, len(conf.Servers))
	sent := 0
	for _, s := range conf.Servers {
		if s.ID == n.id || !s.Voter {
			continue
		}
		sent++
		go func(s Server) {
			cctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.trans.RequestVote(cctx, s, req)
			ch <- result{resp, err}
		}(s)
	}

	for i := 0; i < sent; i++ {
		select {
		case r := <-ch:
			if r.err != nil {
				n.metrics.rpcError("request_vote")
				continue
			}
			n.mu.Lock()
			if r.resp.Term > n.term {
				n.becomeFollowerLocked(r.resp.Term, "")
				n.mu.Unlock()
				return false
			}
			n.mu.Unlock()
			if r.resp.VoteGranted {
				votes++
				if votes >= need {
					return true
				}
			}
		case <-ctx.Done():
			return false
		}
	}
	return false
}

// waitFor re-evaluates cond (with mu held) every time node state moves.
func (n *Node) waitFor(ctx context.Context, cond func() (bool, error)) error {
	for {
		n.mu.Lock()
		ok, err := cond()
		ch := n.changed
		n.mu.Unlock()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrStopped
		}
	}
}

func (n *Node) notLeaderErrLocked() error {
	e := &NotLeaderError{LeaderID: n.leaderID}
	if s, ok := n.conf.Get(n.leaderID); ok {
		e.LeaderAddr = s.Addr
	}
	return e
}

// Propose appends data to the log and blocks until it has been committed and
// applied locally, returning whatever FSM.Apply returned. ErrLeadershipLost
// means the outcome is unknown: the entry may still commit under a new leader.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	p := &proposal{data: data, done: make(chan struct{})}
	select {
	case n.propCh <- p:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopCh:
		return nil, ErrStopped
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopCh:
		return nil, ErrStopped
	}
	if p.err != nil {
		return nil, p.err
	}
	return n.await(ctx, p.f)
}

type proposal struct {
	data []byte
	f    *future
	err  error
	done chan struct{}
}

// group commit: whatever queued up while the last fsync was running goes out
// in one Append, so concurrent clients share a single fsync
func (n *Node) proposeLoop() {
	defer n.wg.Done()
	batch := make([]*proposal, 0, 256)
	for {
		select {
		case p := <-n.propCh:
			batch = append(batch[:0], p)
		case <-n.stopCh:
			return
		}
	drain:
		for len(batch) < cap(batch) {
			select {
			case p := <-n.propCh:
				batch = append(batch, p)
			default:
				break drain
			}
		}
		n.proposeBatch(batch)
	}
}

func (n *Node) proposeBatch(batch []*proposal) {
	n.mu.Lock()
	if err := n.proposableLocked(); err != nil {
		n.mu.Unlock()
		for _, p := range batch {
			p.err = err
			close(p.done)
		}
		return
	}
	ents := make([]Entry, len(batch))
	next := n.rlog.lastIndex() + 1
	for i, p := range batch {
		ents[i] = Entry{Index: next + uint64(i), Term: n.term, Type: EntryCommand, Data: p.data}
	}
	n.must(n.store.Append(ents))
	for i, e := range ents {
		n.rlog.append(e)
		batch[i].f = n.registerLocked(e.Index)
	}
	n.advanceCommitLocked()
	n.kickReplicatorsLocked()
	n.updateMetricsLocked()
	n.mu.Unlock()
	for _, p := range batch {
		close(p.done)
	}
}

func (n *Node) propose(t EntryType, data []byte) (*future, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.proposeLocked(t, data)
}

func (n *Node) proposeLocked(t EntryType, data []byte) (*future, error) {
	if err := n.proposableLocked(); err != nil {
		return nil, err
	}
	f := n.registerLocked(n.appendEntryLocked(t, data))
	n.advanceCommitLocked() // single-voter clusters commit right here
	n.kickReplicatorsLocked()
	n.updateMetricsLocked()
	return f, nil
}

func (n *Node) proposableLocked() error {
	if n.stopped {
		return ErrStopped
	}
	if n.role != Leader {
		n.metrics.proposal("not_leader")
		return n.notLeaderErrLocked()
	}
	if n.transferring {
		return ErrTransferring
	}
	return nil
}

func (n *Node) registerLocked(idx uint64) *future {
	f := &future{term: n.term, start: time.Now(), done: make(chan struct{})}
	n.pending[idx] = f
	return f
}

func (n *Node) await(ctx context.Context, f *future) (any, error) {
	select {
	case <-f.done:
		return f.val, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopCh:
		return nil, ErrStopped
	}
}

// ReadBarrier returns once this node is provably still the leader and has
// applied everything that was committed when the call started, so a read of
// the FSM right after is linearizable (ReadIndex protocol).
func (n *Node) ReadBarrier(ctx context.Context) error {
	var readIndex, term, barrier uint64
	err := n.waitFor(ctx, func() (bool, error) {
		if n.stopped {
			return false, ErrStopped
		}
		if n.role != Leader {
			return false, n.notLeaderErrLocked()
		}
		if n.commitIndex < n.noopIndex {
			return false, nil // wait for our term's first commit
		}
		n.sendSeq++
		readIndex, term, barrier = n.commitIndex, n.term, n.sendSeq
		return true, nil
	})
	if err != nil {
		return err
	}

	n.mu.Lock()
	n.kickReplicatorsLocked()
	n.mu.Unlock()

	err = n.waitFor(ctx, func() (bool, error) {
		if n.stopped {
			return false, ErrStopped
		}
		if n.role != Leader || n.term != term {
			return false, n.notLeaderErrLocked()
		}
		acks := 0
		if n.conf.IsVoter(n.id) {
			acks++
		}
		for _, s := range n.conf.Servers {
			if s.ID == n.id || !s.Voter {
				continue
			}
			if p := n.peers[s.ID]; p != nil && p.lastAck > barrier {
				acks++
			}
		}
		return acks >= n.conf.Quorum() && n.lastApplied >= readIndex, nil
	})
	return err
}
