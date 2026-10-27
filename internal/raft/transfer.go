package raft

import "context"

// TransferLeadership hands leadership to the most caught-up voter and returns
// once this node has stepped down. Used on graceful shutdown so the cluster
// doesn't have to sit through an election timeout. On failure the node just
// carries on as leader.
func (n *Node) TransferLeadership(ctx context.Context) error {
	// while this runs every proposal is refused, so never wait forever
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*n.cfg.ElectionTimeoutMax)
		defer cancel()
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.role != Leader {
		err := n.notLeaderErrLocked()
		n.mu.Unlock()
		return err
	}
	var target *peer
	for _, s := range n.conf.Servers {
		if s.ID == n.id || !s.Voter {
			continue
		}
		if p := n.peers[s.ID]; p != nil && (target == nil || p.matchIndex > target.matchIndex) {
			target = p
		}
	}
	if target == nil {
		n.mu.Unlock()
		return ErrNoTransferTarget
	}
	term := n.term
	n.transferring = true
	n.kickReplicatorsLocked()
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		if n.term == term {
			n.transferring = false
		}
		n.mu.Unlock()
	}()

	// the target has to hold every entry we do, or it would lose the election
	err := n.waitFor(ctx, func() (bool, error) {
		if n.stopped {
			return false, ErrStopped
		}
		if n.role != Leader || n.term != term {
			return false, ErrNotLeader
		}
		return target.matchIndex >= n.rlog.lastIndex(), nil
	})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	_, err = n.trans.TimeoutNow(cctx, target.srv, &TimeoutNowRequest{Term: term, LeaderID: n.id})
	cancel()
	if err != nil {
		return err
	}

	// the target's higher-term vote request demotes us
	return n.waitFor(ctx, func() (bool, error) {
		if n.stopped {
			return false, ErrStopped
		}
		return n.role != Leader || n.term != term, nil
	})
}

// HandleTimeoutNow makes this node call an election immediately, skipping
// the pre-vote round and the wait for its own timer.
func (n *Node) HandleTimeoutNow(req *TimeoutNowRequest) (*TimeoutNowResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	resp := &TimeoutNowResponse{Term: n.term}
	if req.Term != n.term || req.LeaderID != n.leaderID || n.role == Leader || n.campaigning || !n.conf.IsVoter(n.id) {
		return resp, nil
	}
	n.campaigning = true
	n.wg.Add(1)
	go n.campaign(true)
	return resp, nil
}
