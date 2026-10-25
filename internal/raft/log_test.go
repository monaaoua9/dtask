package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func mkLog(snapIdx, snapTerm uint64, terms ...uint64) *raftLog {
	l := &raftLog{snapIndex: snapIdx, snapTerm: snapTerm}
	for i, t := range terms {
		l.append(Entry{Index: snapIdx + uint64(i) + 1, Term: t})
	}
	return l
}

func TestLogIndexMath(t *testing.T) {
	l := mkLog(0, 0, 1, 1, 2, 3)
	require.Equal(t, uint64(4), l.lastIndex())
	require.Equal(t, uint64(3), l.lastTerm())

	tm, ok := l.term(0)
	require.True(t, ok)
	require.Equal(t, uint64(0), tm)
	tm, _ = l.term(3)
	require.Equal(t, uint64(2), tm)
	_, ok = l.term(5)
	require.False(t, ok)
}

func TestLogAfterCompaction(t *testing.T) {
	l := mkLog(0, 0, 1, 1, 2, 2, 3)
	l.compact(3, 2)
	require.Equal(t, uint64(3), l.snapIndex)
	require.Equal(t, uint64(5), l.lastIndex())

	tm, ok := l.term(3)
	require.True(t, ok, "the snapshot boundary keeps its term")
	require.Equal(t, uint64(2), tm)
	_, ok = l.term(2)
	require.False(t, ok, "compacted entries are gone")

	e, ok := l.entry(4)
	require.True(t, ok)
	require.Equal(t, uint64(2), e.Term)
	_, ok = l.entry(3)
	require.False(t, ok)

	l.compact(9, 4) // beyond the end empties the log but moves the boundary
	require.Equal(t, uint64(9), l.lastIndex())
	require.Equal(t, uint64(4), l.lastTerm())
}

func TestLogTruncateAndSliceCopy(t *testing.T) {
	l := mkLog(2, 1, 1, 2, 2, 3) // indexes 3..6
	s := l.slice(4, 7)
	require.Len(t, s, 3)

	l.truncateFrom(5)
	require.Equal(t, uint64(4), l.lastIndex())
	l.append(Entry{Index: 5, Term: 9})
	require.Equal(t, uint64(2), s[1].Term, "slice handed to an RPC must not see later overwrites")

	l.truncateFrom(1) // at or below the snapshot clears everything
	require.Equal(t, uint64(2), l.lastIndex())
	require.Nil(t, l.slice(3, 10))
}

func TestLogTermLookups(t *testing.T) {
	l := mkLog(0, 0, 1, 1, 2, 2, 2, 3)
	require.Equal(t, uint64(3), l.firstIndexOfTerm(2))
	idx, ok := l.lastIndexOfTerm(2)
	require.True(t, ok)
	require.Equal(t, uint64(5), idx)
	_, ok = l.lastIndexOfTerm(7)
	require.False(t, ok)
}

func TestConfigurationQuorumAndEdits(t *testing.T) {
	c := Configuration{Servers: []Server{{ID: "b", Voter: true}, {ID: "a", Voter: true}, {ID: "c", Voter: false}}}
	require.Equal(t, 2, c.Voters())
	require.Equal(t, 2, c.Quorum())

	c = c.with(Server{ID: "c", Voter: true})
	require.Equal(t, 3, c.Voters())
	require.Equal(t, 2, c.Quorum())

	c = c.with(Server{ID: "0", Voter: false})
	require.Equal(t, "0", string(c.Servers[0].ID), "stays sorted so equal configs encode identically")

	c = c.without("b")
	require.False(t, c.IsVoter("b"))

	dec, err := DecodeConfiguration(EncodeConfiguration(c))
	require.NoError(t, err)
	require.Equal(t, c, dec)
}
