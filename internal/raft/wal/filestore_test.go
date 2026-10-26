package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monaaoua9/dtask/internal/raft"
)

func entries(from, to, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Type: raft.EntryCommand, Data: []byte(fmt.Sprint("cmd-", i))})
	}
	return out
}

func open(t *testing.T, dir string) *FileStore {
	t.Helper()
	s, err := Open(dir, Options{NoSync: true})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() }) // windows can't delete a dir with open files
	return s
}

func TestAppendReloadAndTruncate(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.SaveState(raft.PersistentState{Term: 7, VotedFor: "n2"}))
	require.NoError(t, s.Append(entries(1, 10, 3)))
	require.NoError(t, s.TruncateSuffix(8))
	require.NoError(t, s.Append(entries(8, 9, 4)))
	require.NoError(t, s.Close())

	s = open(t, dir)
	st, log, snap, _, err := s.Load()
	require.NoError(t, err)
	require.Nil(t, snap)
	require.Equal(t, raft.PersistentState{Term: 7, VotedFor: "n2"}, st)
	require.Len(t, log, 9)
	require.Equal(t, uint64(3), log[6].Term)
	require.Equal(t, uint64(4), log[7].Term)
	require.Equal(t, []byte("cmd-9"), log[8].Data)

	// appending after reload continues at the right index
	require.NoError(t, s.Append(entries(10, 10, 4)))
	require.Error(t, s.Append(entries(20, 20, 4)), "gaps are refused")
}

func TestTruncateEverything(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Append(entries(1, 5, 1)))
	require.NoError(t, s.TruncateSuffix(1))
	require.NoError(t, s.Append(entries(1, 2, 2)))
	require.NoError(t, s.Close())

	_, log, _, _, err := open(t, dir).Load()
	require.NoError(t, err)
	require.Len(t, log, 2)
	require.Equal(t, uint64(2), log[0].Term)
}

func TestTornWriteAtTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Append(entries(1, 5, 1)))
	require.NoError(t, s.Close())

	// simulate power loss halfway through writing record 6
	f, err := os.OpenFile(filepath.Join(dir, walFile), os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	rec := encodeRecord(entries(6, 6, 1)[0])
	_, err = f.Write(rec[:len(rec)-3])
	require.NoError(t, err)
	require.NoError(t, f.Close())

	s = open(t, dir)
	_, log, _, _, err := s.Load()
	require.NoError(t, err)
	require.Len(t, log, 5)
	require.NoError(t, s.Append(entries(6, 7, 1)), "tail was cut cleanly, appends work")
	require.NoError(t, s.Close())

	_, log, _, _, _ = open(t, dir).Load()
	require.Len(t, log, 7)
}

func TestCorruptRecordInTheMiddleIsRefused(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Append(entries(1, 6, 1)))
	require.NoError(t, s.Close())

	p := filepath.Join(dir, walFile)
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	b[len(b)/2] ^= 0xff // flip a byte somewhere in the middle
	require.NoError(t, os.WriteFile(p, b, 0o644))

	s2, err := Open(dir, Options{NoSync: true})
	if s2 != nil {
		s2.Close()
	}
	require.ErrorIs(t, err, ErrCorrupt, "silently dropping the entries after it could lose committed data")
}

// builds a wal image and the offset where each record starts
func walImage(n int) ([]byte, []int) {
	var img []byte
	var starts []int
	for i := 1; i <= n; i++ {
		starts = append(starts, len(img))
		e := raft.Entry{Index: uint64(i), Term: 1, Type: raft.EntryCommand, Data: bytes.Repeat([]byte{byte(i)}, i*3)}
		img = append(img, encodeRecord(e)...)
	}
	return img, starts
}

func openImage(t *testing.T, img []byte) (*FileStore, error) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, walFile), img, 0o644))
	s, err := Open(dir, Options{NoSync: true})
	if s != nil {
		t.Cleanup(func() { s.Close() })
	}
	return s, err
}

// power can fail after any byte of a write, so every possible length of the
// file must recover to exactly the records that were fully written
func TestEveryTornLengthRecovers(t *testing.T) {
	const n = 8
	img, starts := walImage(n)
	whole := func(l int) int {
		c := 0
		for i := range starts {
			end := len(img)
			if i+1 < len(starts) {
				end = starts[i+1]
			}
			if end <= l {
				c++
			}
		}
		return c
	}

	for l := 0; l <= len(img); l++ {
		s, err := openImage(t, img[:l])
		require.NoError(t, err, "length %d", l)
		_, log, _, _, _ := s.Load()
		want := whole(l)
		require.Len(t, log, want, "length %d", l)
		for i, e := range log {
			require.Equal(t, uint64(i+1), e.Index)
		}
		require.NoError(t, s.Append(entries(uint64(want+1), uint64(want+1), 2)), "length %d: appends after recovery", l)

		// and what we appended must survive another restart, with nothing left from the torn tail
		require.NoError(t, s.Close())
		s2, err := Open(s.dir, Options{NoSync: true})
		require.NoError(t, err, "length %d: reopen", l)
		_, again, _, _, _ := s2.Load()
		require.Len(t, again, want+1, "length %d: reopen", l)
		require.NoError(t, s2.Close())
	}
}

// a flipped byte is either caught as corruption or, if it sits in the last
// record (indistinguishable from a torn write), costs only that record. It can
// never produce a wrong entry.
func TestEveryByteFlipIsDetected(t *testing.T) {
	const n = 8
	img, starts := walImage(n)
	last := starts[n-1]

	for p := 0; p < len(img); p++ {
		bad := append([]byte(nil), img...)
		bad[p] ^= 0xff
		s, err := openImage(t, bad)
		if p < last {
			require.ErrorIs(t, err, ErrCorrupt, "flip at %d", p)
			continue
		}
		require.NoError(t, err, "flip at %d", p)
		_, log, _, _, _ := s.Load()
		require.Len(t, log, n-1, "flip at %d", p)
		for i, e := range log {
			require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, (i+1)*3), e.Data)
		}
	}
}

func TestLeftoverTempFilesAreIgnored(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Append(entries(1, 4, 1)))
	require.NoError(t, s.Close())

	for _, f := range []string{stateFile + ".tmp", snapshotFile + ".tmp", walFile + ".tmp"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f), []byte("half-written garbage"), 0o644))
	}
	_, log, snap, _, err := open(t, dir).Load()
	require.NoError(t, err)
	require.Nil(t, snap)
	require.Len(t, log, 4)
	_, err = os.Stat(filepath.Join(dir, walFile+".tmp"))
	require.True(t, os.IsNotExist(err))
}

func TestSnapshotCompactsLogAndSurvivesReload(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Append(entries(1, 20, 2)))
	meta := raft.SnapshotMeta{Index: 12, Term: 2, Configuration: raft.Configuration{Servers: []raft.Server{{ID: "a", Addr: "a:1", Voter: true}}}}
	require.NoError(t, s.SaveSnapshot(meta, []byte("state@12")))
	require.NoError(t, s.Append(entries(21, 22, 2)))
	require.NoError(t, s.TruncateSuffix(21))
	require.NoError(t, s.Append(entries(21, 21, 3)))
	require.NoError(t, s.Close())

	s = open(t, dir)
	_, log, gotMeta, data, err := s.Load()
	require.NoError(t, err)
	require.Equal(t, meta, *gotMeta)
	require.Equal(t, []byte("state@12"), data)
	require.Len(t, log, 9) // 13..21
	require.Equal(t, uint64(13), log[0].Index)
	require.Equal(t, uint64(3), log[8].Term)
}

func TestSnapshotBeyondLogDropsEverything(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Append(entries(1, 5, 1)))
	require.NoError(t, s.SaveSnapshot(raft.SnapshotMeta{Index: 50, Term: 4}, []byte("big")))
	require.NoError(t, s.Append(entries(51, 52, 4)))
	require.NoError(t, s.Close())

	_, log, meta, _, err := open(t, dir).Load()
	require.NoError(t, err)
	require.Equal(t, uint64(50), meta.Index)
	require.Len(t, log, 2)
	require.Equal(t, uint64(51), log[0].Index)
}

func TestCrashBetweenSnapshotAndWalTrim(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Append(entries(1, 10, 1)))
	require.NoError(t, s.Close())

	// snapshot file landed, wal never got trimmed
	side := open(t, t.TempDir())
	require.NoError(t, side.SaveSnapshot(raft.SnapshotMeta{Index: 6, Term: 1}, []byte("x")))
	require.NoError(t, side.Close())
	b, err := os.ReadFile(filepath.Join(side.dir, snapshotFile))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, snapshotFile), b, 0o644))

	s = open(t, dir)
	_, log, meta, _, err := s.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(6), meta.Index)
	require.Len(t, log, 4)
	require.Equal(t, uint64(7), log[0].Index)
	require.NoError(t, s.Append(entries(11, 11, 1)))
}

func TestCorruptSnapshotIsAnError(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.SaveSnapshot(raft.SnapshotMeta{Index: 3, Term: 1}, []byte("payload")))
	require.NoError(t, s.Close())

	p := filepath.Join(dir, snapshotFile)
	b, _ := os.ReadFile(p)
	b[len(b)-6] ^= 0xff
	require.NoError(t, os.WriteFile(p, b, 0o644))
	s2, err := Open(dir, Options{NoSync: true})
	if s2 != nil {
		s2.Close()
	}
	require.Error(t, err)
}

// snapshot landed past the end of the wal, then power died before the wal was
// rewritten. Two restarts later the node used to refuse to start.
func TestSnapshotAheadOfWalSurvivesTwoRestarts(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Append(entries(1, 5, 1)))
	require.NoError(t, s.Close())

	side := open(t, t.TempDir())
	require.NoError(t, side.SaveSnapshot(raft.SnapshotMeta{Index: 9, Term: 1}, []byte("x")))
	require.NoError(t, side.Close())
	b, err := os.ReadFile(filepath.Join(side.dir, snapshotFile))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, snapshotFile), b, 0o644))

	s = open(t, dir)
	_, log, meta, _, err := s.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(9), meta.Index)
	require.Empty(t, log)
	require.NoError(t, s.Append(entries(10, 11, 2)))
	require.NoError(t, s.Close())

	s = open(t, dir)
	_, log, _, _, err = s.Load()
	require.NoError(t, err, "second restart")
	require.Len(t, log, 2)
	require.Equal(t, uint64(10), log[0].Index)
}
