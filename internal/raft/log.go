package raft

// raftLog is the in-memory view of the log. Everything at or before
// snapIndex has been folded into a snapshot; entries[i] holds index
// snapIndex+1+i. Not safe for concurrent use, the node lock guards it.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// term returns the term at index i. Index 0 is the empty-log sentinel.
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	}
	return l.entries[i-l.snapIndex-1].Term, true
}

func (l *raftLog) entry(i uint64) (Entry, bool) {
	if i <= l.snapIndex || i > l.lastIndex() {
		return Entry{}, false
	}
	return l.entries[i-l.snapIndex-1], true
}

// slice returns a copy of [lo, hi). Copying matters: the backing array can be
// truncated and overwritten while an RPC built from it is still in flight.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo <= l.snapIndex {
		lo = l.snapIndex + 1
	}
	if hi > l.lastIndex()+1 {
		hi = l.lastIndex() + 1
	}
	if lo >= hi {
		return nil
	}
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-l.snapIndex-1:hi-l.snapIndex-1])
	return out
}

func (l *raftLog) append(es ...Entry) { l.entries = append(l.entries, es...) }

// truncateFrom drops index i and everything after it.
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.snapIndex {
		l.entries = nil
		return
	}
	if i > l.lastIndex() {
		return
	}
	l.entries = l.entries[:i-l.snapIndex-1]
}

// compact folds everything up to index into the snapshot boundary. If the log
// is shorter than index the whole log is dropped.
func (l *raftLog) compact(index, term uint64) {
	if index <= l.snapIndex {
		return
	}
	if index >= l.lastIndex() {
		l.entries = nil
	} else {
		rest := l.entries[index-l.snapIndex:]
		l.entries = append([]Entry(nil), rest...)
	}
	l.snapIndex, l.snapTerm = index, term
}

func (l *raftLog) firstIndexOfTerm(term uint64) uint64 {
	for _, e := range l.entries {
		if e.Term == term {
			return e.Index
		}
	}
	return l.snapIndex + 1
}

func (l *raftLog) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term == term {
			return l.entries[i].Index, true
		}
	}
	return 0, false
}
