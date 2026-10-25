package raft

import "context"

// AddServer brings a new node into the cluster in two steps: first as a
// non-voting learner so it can catch up without affecting quorum, then as a
// full voter once it has the leader's log. Safe to retry.
func (n *Node) AddServer(ctx context.Context, id NodeID, addr string) error {
	if err := n.lockConfChange(ctx); err != nil {
		return err
	}
	defer n.unlockConfChange()

	n.mu.Lock()
	if err := n.configChangeAllowedLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	cur, known := n.conf.Get(id)
	if known && cur.Voter {
		n.mu.Unlock()
		return nil
	}
	if !known {
		cur = Server{ID: id, Addr: addr}
		f, err := n.proposeConfigLocked(n.conf.with(cur))
		n.mu.Unlock()
		if err != nil {
			return err
		}
		if _, err := n.await(ctx, f); err != nil {
			return err
		}
	} else {
		n.mu.Unlock()
	}

	// wait for the learner to reach where the leader's log is right now
	n.mu.Lock()
	target := n.rlog.lastIndex()
	n.mu.Unlock()
	err := n.waitFor(ctx, func() (bool, error) {
		if n.stopped {
			return false, ErrStopped
		}
		if n.role != Leader {
			return false, n.notLeaderErrLocked()
		}
		p := n.peers[id]
		return p != nil && p.matchIndex >= target, nil
	})
	if err != nil {
		return err
	}

	n.mu.Lock()
	if err := n.configChangeAllowedLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	cur.Voter = true
	f, err := n.proposeConfigLocked(n.conf.with(cur))
	n.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = n.await(ctx, f)
	return err
}

// RemoveServer drops a node from the cluster. Removing the leader itself is
// allowed; it steps down once the change commits.
func (n *Node) RemoveServer(ctx context.Context, id NodeID) error {
	if err := n.lockConfChange(ctx); err != nil {
		return err
	}
	defer n.unlockConfChange()

	n.mu.Lock()
	if err := n.configChangeAllowedLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	s, ok := n.conf.Get(id)
	if !ok {
		n.mu.Unlock()
		return nil
	}
	next := n.conf.without(id)
	if s.Voter && next.Voters() == 0 {
		n.mu.Unlock()
		return ErrLastVoter
	}
	f, err := n.proposeConfigLocked(next)
	n.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = n.await(ctx, f)
	return err
}

func (n *Node) lockConfChange(ctx context.Context) error {
	select {
	case n.confSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrStopped
	}
}

func (n *Node) unlockConfChange() { <-n.confSem }

// one change at a time, and only after the previous one committed: that is
// what makes single-server changes safe without joint consensus
func (n *Node) configChangeAllowedLocked() error {
	if n.stopped {
		return ErrStopped
	}
	if n.role != Leader {
		return n.notLeaderErrLocked()
	}
	if n.commitIndex < n.noopIndex {
		return ErrLeaderNotReady
	}
	if n.confIndex > n.commitIndex {
		return ErrConfigInProgress
	}
	return nil
}

func (n *Node) proposeConfigLocked(c Configuration) (*future, error) {
	if err := n.proposableLocked(); err != nil {
		return nil, err
	}
	idx := n.appendEntryLocked(EntryConfig, EncodeConfiguration(c))
	f := n.registerLocked(idx)
	// a config takes effect the moment it's appended, committed or not, and
	// it has to be committed by a majority of the *new* config
	n.conf, n.confIndex = c, idx
	n.pruneAndStartPeersLocked()
	n.advanceCommitLocked()
	n.kickReplicatorsLocked()
	n.updateMetricsLocked()
	return f, nil
}

// pruneAndStartPeersLocked lines the replicators up with the current config.
// A removed peer is only dropped once the change that removed it is committed,
// so it has had a chance to learn about it.
func (n *Node) pruneAndStartPeersLocked() {
	last := n.rlog.lastIndex()
	for _, s := range n.conf.Servers {
		if s.ID == n.id {
			continue
		}
		if _, ok := n.peers[s.ID]; !ok {
			n.startPeerLocked(s, last)
		}
	}
	if n.confIndex > n.commitIndex {
		return
	}
	for id, p := range n.peers {
		if _, ok := n.conf.Get(id); !ok {
			p.cancel()
			delete(n.peers, id)
		}
	}
}
