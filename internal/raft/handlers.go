package raft

import (
	"fmt"
	"time"
)

var _ RPCHandler = (*Node)(nil)

func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &RequestVoteResponse{Term: n.term}
	if n.stopped {
		return resp
	}

	// leader stickiness: while we still hear from a live leader, ignore
	// candidates entirely. This is what stops a node coming back from a
	// partition (or a removed one) from bumping the term and deposing a
	// healthy leader.
	recent := time.Since(n.lastLeaderContact) < n.cfg.ElectionTimeoutMin
	if !req.LeadershipTransfer && (n.role == Leader || recent) {
		return resp
	}
	if len(n.conf.Servers) > 0 && !n.conf.IsVoter(req.CandidateID) {
		return resp
	}
	if req.Term < n.term {
		return resp
	}

	if req.PreVote {
		resp.VoteGranted = n.logUpToDateLocked(req.LastLogIndex, req.LastLogTerm)
		return resp
	}

	if req.Term > n.term {
		n.becomeFollowerLocked(req.Term, "")
		resp.Term = n.term
	}
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && n.logUpToDateLocked(req.LastLogIndex, req.LastLogTerm) {
		n.votedFor = req.CandidateID
		n.persistStateLocked()
		n.resetElectionTimerLocked()
		resp.VoteGranted = true
	}
	return resp
}

func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}

	resp := &AppendEntriesResponse{Term: n.term}
	if req.Term < n.term {
		return resp, nil
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
		resp.Term = n.term
	}
	n.leaderID = req.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionTimerLocked()

	prev, prevTerm, entries := req.PrevLogIndex, req.PrevLogTerm, req.Entries
	if prev < n.rlog.snapIndex {
		// the front of this batch is already inside our snapshot
		skip := n.rlog.snapIndex - prev
		if skip >= uint64(len(entries)) {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prev, prevTerm = n.rlog.snapIndex, n.rlog.snapTerm
	}

	if prev > n.rlog.lastIndex() {
		resp.ConflictIndex = n.rlog.lastIndex() + 1
		return resp, nil
	}
	if t, _ := n.rlog.term(prev); t != prevTerm {
		resp.ConflictTerm = t
		resp.ConflictIndex = n.rlog.firstIndexOfTerm(t)
		return resp, nil
	}

	configTouched := false
	var fresh []Entry
	for i, e := range entries {
		if e.Index <= n.rlog.lastIndex() {
			if t, _ := n.rlog.term(e.Index); t == e.Term {
				continue
			}
			if e.Index <= n.commitIndex {
				panic(fmt.Sprintf("raft %s: leader asked to overwrite committed index %d", n.id, e.Index))
			}
			n.must(n.store.TruncateSuffix(e.Index))
			n.rlog.truncateFrom(e.Index)
			configTouched = true // a rolled-back entry might have been a config
		}
		fresh = entries[i:]
		break
	}
	if len(fresh) > 0 {
		n.must(n.store.Append(fresh))
		n.rlog.append(fresh...)
		for _, e := range fresh {
			if e.Type == EntryConfig {
				configTouched = true
			}
		}
	}
	if configTouched {
		n.recomputeConfigLocked()
	}

	match := prev + uint64(len(entries))
	if req.LeaderCommit > n.commitIndex {
		c := req.LeaderCommit
		if match < c {
			c = match
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.signalApply()
		}
	}
	n.updateMetricsLocked()
	resp.Success = true
	resp.MatchIndex = match
	return resp, nil
}

func (n *Node) HandleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}

	resp := &InstallSnapshotResponse{Term: n.term}
	if req.Term < n.term {
		return resp, nil
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
		resp.Term = n.term
	}
	n.leaderID = req.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionTimerLocked()

	idx := req.LastIncludedIndex
	if idx <= n.commitIndex {
		return resp, nil // we already have everything this covers
	}

	meta := SnapshotMeta{Index: idx, Term: req.LastIncludedTerm, Configuration: req.Configuration.Clone()}
	_, haveIt := n.rlog.term(idx)
	if t, _ := n.rlog.term(idx); !haveIt || t != req.LastIncludedTerm {
		// our log disagrees with (or doesn't reach) the snapshot point; it gets
		// dropped, but only after the snapshot is durable below
		n.rlog.entries = nil
		n.rlog.snapIndex, n.rlog.snapTerm = idx, req.LastIncludedTerm
		n.must(n.store.SaveSnapshot(meta, req.Data))
		n.must(n.store.TruncateSuffix(idx + 1))
	} else {
		n.must(n.store.SaveSnapshot(meta, req.Data))
		n.rlog.compact(idx, req.LastIncludedTerm)
	}
	if err := n.fsm.Restore(req.Data); err != nil {
		n.must(fmt.Errorf("restore installed snapshot: %w", err))
	}

	n.snapMeta, n.snapData = meta, req.Data
	n.appliedConf = meta.Configuration.Clone()
	n.lastApplied, n.commitIndex = idx, idx
	n.recomputeConfigLocked()
	n.updateMetricsLocked()
	n.notifyLocked()
	n.signalApply()
	return resp, nil
}
