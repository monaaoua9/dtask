package raft

import "time"

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		}
		n.applyMu.Lock()
		n.applyCommitted()
		n.maybeSnapshot()
		n.applyMu.Unlock()
	}
}

// applyCommitted drains committed entries into the FSM. Caller holds applyMu.
func (n *Node) applyCommitted() {
	const batch = 128
	for {
		n.mu.Lock()
		if n.stopped || n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		lo := n.lastApplied + 1
		hi := n.commitIndex
		if hi-lo >= batch {
			hi = lo + batch - 1
		}
		ents := n.rlog.slice(lo, hi+1)
		n.mu.Unlock()

		for _, e := range ents {
			var res any
			if e.Type == EntryCommand {
				res = n.fsm.Apply(e.Index, e.Data)
			}
			n.mu.Lock()
			n.lastApplied = e.Index
			n.afterApplyLocked(e, res)
			n.mu.Unlock()
		}
	}
}

func (n *Node) afterApplyLocked(e Entry, res any) {
	var resign bool
	if e.Type == EntryConfig {
		c, err := DecodeConfiguration(e.Data)
		n.must(err)
		n.appliedConf = c
		if n.role == Leader {
			n.pruneAndStartPeersLocked()
			_, still := c.Get(n.id)
			resign = !still && e.Index == n.confIndex
		}
	}
	if f, ok := n.pending[e.Index]; ok {
		delete(n.pending, e.Index)
		if f.term == e.Term {
			n.metrics.proposal("committed")
			n.metrics.observeCommit(time.Since(f.start).Seconds())
			f.resolve(res, nil)
		} else {
			n.metrics.proposal("overwritten")
			f.resolve(nil, ErrLeadershipLost)
		}
	}
	// after resolving futures, otherwise the removal itself would report
	// "leadership lost" to whoever asked for it
	if resign {
		n.log.Info("removed from cluster, resigning", "term", n.term)
		n.becomeFollowerLocked(n.term, "")
	}
	n.updateMetricsLocked()
	n.notifyLocked()
}

// maybeSnapshot compacts the log once enough entries have been applied.
// Caller holds applyMu, so the FSM can't move while Snapshot() runs.
func (n *Node) maybeSnapshot() {
	n.mu.Lock()
	if n.stopped || n.cfg.SnapshotThreshold == 0 || n.lastApplied-n.rlog.snapIndex < n.cfg.SnapshotThreshold {
		n.mu.Unlock()
		return
	}
	idx := n.lastApplied
	term, _ := n.rlog.term(idx)
	conf := n.appliedConf.Clone()
	n.mu.Unlock()

	data, err := n.fsm.Snapshot()
	if err != nil {
		n.log.Error("fsm snapshot failed", "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	meta := SnapshotMeta{Index: idx, Term: term, Configuration: conf}
	n.must(n.store.SaveSnapshot(meta, data))
	n.rlog.compact(idx, term)
	n.snapMeta, n.snapData = meta, data
	n.metrics.snapshotTaken()
	n.log.Debug("log compacted", "index", idx, "bytes", len(data))
}
