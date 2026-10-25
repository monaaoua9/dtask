package raft

import (
	"fmt"
	"sort"
	"time"
)

type PeerStatus struct {
	ID          NodeID
	MatchIndex  uint64
	NextIndex   uint64
	LastContact time.Time
}

type Status struct {
	ID           NodeID
	Role         Role
	Term         uint64
	LeaderID     NodeID
	LeaderAddr   string
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
	Config       Configuration
	// Peers is only populated while this node is the leader
	Peers []PeerStatus
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	st := Status{
		ID: n.id, Role: n.role, Term: n.term, LeaderID: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastLogIndex: n.rlog.lastIndex(), Config: n.conf.Clone(),
	}
	if s, ok := n.conf.Get(n.leaderID); ok {
		st.LeaderAddr = s.Addr
	}
	for id, p := range n.peers {
		st.Peers = append(st.Peers, PeerStatus{ID: id, MatchIndex: p.matchIndex, NextIndex: p.nextIndex, LastContact: p.lastContact})
	}
	sort.Slice(st.Peers, func(i, j int) bool { return st.Peers[i].ID < st.Peers[j].ID })
	return st
}

func (n *Node) ID() NodeID { return n.id }

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader && !n.stopped
}

// Term and leadership as one atomic read, handy for election-safety checks.
func (n *Node) Leadership() (term uint64, leader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term, n.role == Leader && !n.stopped
}

// Debug is a one-line dump of internal election state for test failures.
func (n *Node) Debug() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return fmt.Sprintf("%s role=%s term=%d votedFor=%q leader=%q campaigning=%v electionIn=%v sinceLeaderContact=%v last=(%d,%d) conf=%v",
		n.id, n.role, n.term, n.votedFor, n.leaderID, n.campaigning,
		time.Until(n.electionDeadline).Round(time.Millisecond), time.Since(n.lastLeaderContact).Round(time.Millisecond),
		n.rlog.lastIndex(), n.rlog.lastTerm(), n.conf.Servers)
}
