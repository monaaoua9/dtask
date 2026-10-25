package raft

import (
	"context"
	"sort"
	"time"
)

func (n *Node) startPeerLocked(s Server, lastIndex uint64) {
	p := &peer{
		srv:         s,
		nextIndex:   lastIndex + 1,
		trigger:     make(chan struct{}, 1),
		lastContact: time.Now(),
	}
	n.peers[s.ID] = p
	ctx, cancel := context.WithCancel(n.leaderCtx)
	p.cancel = cancel
	term := n.term
	n.wg.Add(1)
	go n.replicator(ctx, p, term)
}

func (n *Node) kickReplicatorsLocked() {
	for _, p := range n.peers {
		select {
		case p.trigger <- struct{}{}:
		default:
		}
	}
}

func (n *Node) replicator(ctx context.Context, p *peer, term uint64) {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		n.replicateOnce(ctx, p, term)
		select {
		case <-ctx.Done():
			return
		case <-p.trigger:
		case <-t.C:
		}
	}
}

// replicateOnce pushes entries (or a snapshot) to one follower until it's
// caught up, a call fails, or we stop being leader.
func (n *Node) replicateOnce(ctx context.Context, p *peer, term uint64) {
	for ctx.Err() == nil {
		n.mu.Lock()
		if n.role != Leader || n.term != term {
			n.mu.Unlock()
			return
		}
		n.sendSeq++
		seq := n.sendSeq
		if p.nextIndex <= n.rlog.snapIndex {
			req := &InstallSnapshotRequest{
				Term: term, LeaderID: n.id,
				LastIncludedIndex: n.snapMeta.Index, LastIncludedTerm: n.snapMeta.Term,
				Configuration: n.snapMeta.Configuration.Clone(), Data: n.snapData,
			}
			n.mu.Unlock()
			if !n.sendSnapshot(ctx, p, term, seq, req) {
				return
			}
			continue
		}

		prev := p.nextIndex - 1
		prevTerm, _ := n.rlog.term(prev)
		last := n.rlog.lastIndex()
		hi := p.nextIndex + uint64(n.cfg.MaxAppendEntries)
		if hi > last+1 {
			hi = last + 1
		}
		req := &AppendEntriesRequest{
			Term: term, LeaderID: n.id,
			PrevLogIndex: prev, PrevLogTerm: prevTerm,
			Entries:      n.rlog.slice(p.nextIndex, hi),
			LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()

		cctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
		resp, err := n.trans.AppendEntries(cctx, p.srv, req)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				n.metrics.rpcError("append_entries")
			}
			return
		}

		n.mu.Lock()
		if n.role != Leader || n.term != term {
			n.mu.Unlock()
			return
		}
		if resp.Term > n.term {
			n.becomeFollowerLocked(resp.Term, "")
			n.mu.Unlock()
			return
		}
		p.lastAck, p.lastContact = seq, time.Now()

		if resp.Success {
			if m := prev + uint64(len(req.Entries)); m > p.matchIndex {
				p.matchIndex = m
			}
			p.nextIndex = p.matchIndex + 1
			n.advanceCommitLocked()
			more := p.nextIndex <= n.rlog.lastIndex()
			n.notifyLocked()
			n.mu.Unlock()
			if !more {
				return
			}
			continue
		}

		next := resp.ConflictIndex
		if resp.ConflictTerm != 0 {
			if idx, ok := n.rlog.lastIndexOfTerm(resp.ConflictTerm); ok {
				next = idx + 1
			}
		}
		if next >= p.nextIndex {
			next = p.nextIndex - 1
		}
		if next < 1 {
			next = 1
		}
		p.nextIndex = next
		n.notifyLocked()
		n.mu.Unlock()
	}
}

func (n *Node) sendSnapshot(ctx context.Context, p *peer, term, seq uint64, req *InstallSnapshotRequest) bool {
	// snapshots can be big, give them more room than a heartbeat
	cctx, cancel := context.WithTimeout(ctx, 10*n.cfg.RPCTimeout)
	defer cancel()
	resp, err := n.trans.InstallSnapshot(cctx, p.srv, req)
	if err != nil {
		if ctx.Err() == nil {
			n.metrics.rpcError("install_snapshot")
		}
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.term != term {
		return false
	}
	if resp.Term > n.term {
		n.becomeFollowerLocked(resp.Term, "")
		return false
	}
	p.lastAck, p.lastContact = seq, time.Now()
	if req.LastIncludedIndex > p.matchIndex {
		p.matchIndex = req.LastIncludedIndex
	}
	p.nextIndex = p.matchIndex + 1
	n.metrics.snapshotSent()
	n.notifyLocked()
	return true
}

// advanceCommitLocked moves commitIndex to the highest index stored on a
// majority of voters, as long as that entry is from the current term.
func (n *Node) advanceCommitLocked() {
	if n.role != Leader {
		return
	}
	var matches []uint64
	for _, s := range n.conf.Servers {
		if !s.Voter {
			continue
		}
		if s.ID == n.id {
			matches = append(matches, n.rlog.lastIndex())
		} else if p := n.peers[s.ID]; p != nil {
			matches = append(matches, p.matchIndex)
		} else {
			matches = append(matches, 0)
		}
	}
	q := n.conf.Quorum()
	if len(matches) < q {
		return
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	idx := matches[q-1]
	if idx <= n.commitIndex {
		return
	}
	if t, ok := n.rlog.term(idx); !ok || t != n.term {
		return
	}
	n.commitIndex = idx
	n.updateMetricsLocked()
	n.notifyLocked()
	n.signalApply()
}
