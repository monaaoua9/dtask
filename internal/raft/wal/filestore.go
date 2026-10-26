// Package wal is the on-disk raft.Store: an append-only log of CRC-framed
// records, an atomically replaced state file and a single-file snapshot.
package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/monaaoua9/dtask/internal/raft"
)

const (
	stateFile    = "state.json"
	walFile      = "wal.log"
	snapshotFile = "snapshot.bin"
	snapMagic    = 0x44545350 // "DTSP"
	recHeader    = 8          // len + crc
)

// ErrCorrupt means a record failed its checks but intact records follow it, so
// this isn't a torn tail from a crash: entries that were acknowledged would be
// lost if we carried on. Wipe the data dir and let the node catch up again.
var ErrCorrupt = errors.New("wal: corrupt record in the middle of the log")

type Options struct {
	// NoSync skips fsync. Only for tests and benchmarks: a power cut can eat acked writes.
	NoSync bool
}

type FileStore struct {
	dir  string
	opts Options

	mu      sync.Mutex
	f       *os.File
	first   uint64  // index of the first record in the wal
	offsets []int64 // file offset of each record, offsets[i] holds index first+i
	size    int64

	// filled by Open, handed over (and dropped) by Load
	loadedState raft.PersistentState
	loadedLog   []raft.Entry
	loadedSnap  *raft.SnapshotMeta
	loadedData  []byte
}

func Open(dir string, opts Options) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStore{dir: dir, opts: opts}

	// leftovers from a crash mid-rename are garbage by definition
	for _, tmp := range []string{stateFile + ".tmp", snapshotFile + ".tmp", walFile + ".tmp"} {
		_ = os.Remove(filepath.Join(dir, tmp))
	}

	if err := s.readState(); err != nil {
		return nil, err
	}
	if err := s.readSnapshot(); err != nil {
		return nil, err
	}
	if err := s.openWAL(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *FileStore) path(name string) string { return filepath.Join(s.dir, name) }

func (s *FileStore) readState() error {
	b, err := os.ReadFile(s.path(stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var disk struct {
		Term     uint64 `json:"term"`
		VotedFor string `json:"voted_for"`
	}
	if err := json.Unmarshal(b, &disk); err != nil {
		return fmt.Errorf("wal: corrupt %s: %w", stateFile, err)
	}
	s.loadedState = raft.PersistentState{Term: disk.Term, VotedFor: raft.NodeID(disk.VotedFor)}
	return nil
}

func (s *FileStore) SaveState(st raft.PersistentState) error {
	b, _ := json.Marshal(struct {
		Term     uint64 `json:"term"`
		VotedFor string `json:"voted_for"`
	}{st.Term, string(st.VotedFor)})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.atomicWrite(stateFile, b)
}

func (s *FileStore) atomicWrite(name string, data []byte) error {
	tmp := s.path(name + ".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if !s.opts.NoSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path(name)); err != nil {
		return err
	}
	s.syncDir()
	return nil
}

// directory fsync isn't supported everywhere (windows), so failures are ignored
func (s *FileStore) syncDir() {
	if s.opts.NoSync {
		return
	}
	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

func (s *FileStore) writeSynced(name string, data []byte) error {
	f, err := os.OpenFile(s.path(name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if !s.opts.NoSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

func (s *FileStore) readSnapshot() error {
	b, err := os.ReadFile(s.path(snapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// layout: magic u32 | metaLen u32 | meta json | dataLen u64 | data | crc32 of everything before
	if len(b) < 20 || binary.LittleEndian.Uint32(b) != snapMagic {
		return fmt.Errorf("wal: bad snapshot header")
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return fmt.Errorf("wal: snapshot checksum mismatch")
	}
	metaLen := int(binary.LittleEndian.Uint32(b[4:]))
	if 8+metaLen+8 > len(body) {
		return fmt.Errorf("wal: snapshot truncated")
	}
	var meta raft.SnapshotMeta
	if err := json.Unmarshal(b[8:8+metaLen], &meta); err != nil {
		return fmt.Errorf("wal: snapshot meta: %w", err)
	}
	dataLen := int(binary.LittleEndian.Uint64(b[8+metaLen:]))
	start := 8 + metaLen + 8
	if start+dataLen != len(body) {
		return fmt.Errorf("wal: snapshot length mismatch")
	}
	s.loadedSnap = &meta
	s.loadedData = append([]byte(nil), body[start:]...)
	return nil
}

func encodeSnapshot(meta raft.SnapshotMeta, data []byte) []byte {
	mb, _ := json.Marshal(meta)
	var buf bytes.Buffer
	var u32 [4]byte
	var u64 [8]byte
	binary.LittleEndian.PutUint32(u32[:], snapMagic)
	buf.Write(u32[:])
	binary.LittleEndian.PutUint32(u32[:], uint32(len(mb)))
	buf.Write(u32[:])
	buf.Write(mb)
	binary.LittleEndian.PutUint64(u64[:], uint64(len(data)))
	buf.Write(u64[:])
	buf.Write(data)
	binary.LittleEndian.PutUint32(u32[:], crc32.ChecksumIEEE(buf.Bytes()))
	buf.Write(u32[:])
	return buf.Bytes()
}

func encodeRecord(e raft.Entry) []byte {
	payload := make([]byte, 17+len(e.Data))
	binary.LittleEndian.PutUint64(payload[0:], e.Index)
	binary.LittleEndian.PutUint64(payload[8:], e.Term)
	payload[16] = byte(e.Type)
	copy(payload[17:], e.Data)
	rec := make([]byte, recHeader+len(payload))
	binary.LittleEndian.PutUint32(rec[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(rec[4:], crc32.ChecksumIEEE(payload))
	copy(rec[recHeader:], payload)
	return rec
}

func (s *FileStore) openWAL() error {
	f, err := os.OpenFile(s.path(walFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return err
	}

	var (
		entries []raft.Entry
		offsets []int64
		pos     int64
	)
	for int64(len(raw))-pos >= recHeader {
		n := int64(binary.LittleEndian.Uint32(raw[pos:]))
		sum := binary.LittleEndian.Uint32(raw[pos+4:])
		end := pos + recHeader + n
		var e raft.Entry
		good := n >= 17 && end <= int64(len(raw)) && crc32.ChecksumIEEE(raw[pos+recHeader:end]) == sum
		if good {
			payload := raw[pos+recHeader : end]
			e = raft.Entry{
				Index: binary.LittleEndian.Uint64(payload[0:]),
				Term:  binary.LittleEndian.Uint64(payload[8:]),
				Type:  raft.EntryType(payload[16]),
				Data:  append([]byte(nil), payload[17:]...),
			}
			if len(entries) > 0 && e.Index != entries[len(entries)-1].Index+1 {
				f.Close()
				return fmt.Errorf("%w: index %d follows %d at offset %d", ErrCorrupt, e.Index, entries[len(entries)-1].Index, pos)
			}
		} else if intactRecordAfter(raw, pos+1) {
			f.Close()
			return fmt.Errorf("%w (offset %d)", ErrCorrupt, pos)
		} else {
			break
		}
		entries = append(entries, e)
		offsets = append(offsets, pos)
		pos = end
	}
	// a torn tail (crash mid-write) is expected; cut it off so appends start clean
	if pos != int64(len(raw)) {
		if err := f.Truncate(pos); err != nil {
			f.Close()
			return err
		}
	}

	// drop what the snapshot already covers, and give up on the rest if
	// there's a gap between the two
	if s.loadedSnap != nil {
		cut := 0
		for cut < len(entries) && entries[cut].Index <= s.loadedSnap.Index {
			cut++
		}
		entries, offsets = entries[cut:], offsets[cut:]
		gap := len(entries) > 0 && entries[0].Index != s.loadedSnap.Index+1
		// nothing usable left: wipe the file too, or the next append would land
		// behind stale records and the following Open would call it corruption
		if gap || len(entries) == 0 {
			entries, offsets = nil, nil
			if err := f.Truncate(0); err != nil {
				f.Close()
				return err
			}
			pos = 0
		}
	}

	if !s.opts.NoSync {
		_ = f.Sync() // make the truncation above stick before appends resume
	}
	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		f.Close()
		return err
	}
	s.f, s.offsets, s.size = f, offsets, pos
	if len(entries) > 0 {
		s.first = entries[0].Index
	}
	s.loadedLog = entries
	s.syncDir() // the wal.log we may just have created has to survive power loss too
	return nil
}

// a crash can only damage the end of the file, so a valid record further on
// proves the damage is somewhere else
func intactRecordAfter(raw []byte, from int64) bool {
	for o := from; o+recHeader <= int64(len(raw)); o++ {
		n := int64(binary.LittleEndian.Uint32(raw[o:]))
		end := o + recHeader + n
		if n < 17 || end > int64(len(raw)) {
			continue
		}
		if crc32.ChecksumIEEE(raw[o+recHeader:end]) == binary.LittleEndian.Uint32(raw[o+4:]) {
			return true
		}
	}
	return false
}

func (s *FileStore) Load() (raft.PersistentState, []raft.Entry, *raft.SnapshotMeta, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, snap, data := s.loadedLog, s.loadedSnap, s.loadedData
	s.loadedLog, s.loadedData = nil, nil
	return s.loadedState, log, snap, data, nil
}

func (s *FileStore) Append(es []raft.Entry) error {
	if len(es) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.offsets) == 0 {
		s.first = es[0].Index
	} else if want := s.first + uint64(len(s.offsets)); es[0].Index != want {
		return fmt.Errorf("wal: append index %d, want %d", es[0].Index, want)
	}

	var buf bytes.Buffer
	pos := s.size
	offs := make([]int64, 0, len(es))
	for _, e := range es {
		rec := encodeRecord(e)
		offs = append(offs, pos)
		pos += int64(len(rec))
		buf.Write(rec)
	}
	if _, err := s.f.Write(buf.Bytes()); err != nil {
		return err
	}
	if !s.opts.NoSync {
		if err := s.f.Sync(); err != nil {
			return err
		}
	}
	s.offsets = append(s.offsets, offs...)
	s.size = pos
	return nil
}

func (s *FileStore) TruncateSuffix(from uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.offsets) == 0 || from >= s.first+uint64(len(s.offsets)) {
		return nil
	}
	var keep int
	if from > s.first {
		keep = int(from - s.first)
	}
	cut := s.size
	if keep < len(s.offsets) {
		cut = s.offsets[keep]
	}
	if err := s.f.Truncate(cut); err != nil {
		return err
	}
	if _, err := s.f.Seek(cut, io.SeekStart); err != nil {
		return err
	}
	if !s.opts.NoSync {
		if err := s.f.Sync(); err != nil {
			return err
		}
	}
	s.offsets, s.size = s.offsets[:keep], cut
	return nil
}

func (s *FileStore) SaveSnapshot(meta raft.SnapshotMeta, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// snapshot first: if we die before the wal is trimmed, Open ignores the
	// stale prefix
	if err := s.atomicWrite(snapshotFile, encodeSnapshot(meta, data)); err != nil {
		return err
	}

	last := s.first + uint64(len(s.offsets)) // one past the last stored index
	var tail []byte
	if len(s.offsets) > 0 && meta.Index+1 < last {
		from := int64(0)
		if meta.Index+1 > s.first {
			from = s.offsets[meta.Index+1-s.first]
		}
		tail = make([]byte, s.size-from)
		if _, err := s.f.ReadAt(tail, from); err != nil {
			return err
		}
	}

	// rewrite the wal with only what's left; windows won't rename over an open file
	if err := s.f.Close(); err != nil {
		return err
	}
	if err := s.writeSynced(walFile+".tmp", tail); err != nil {
		return err
	}
	if err := os.Rename(s.path(walFile+".tmp"), s.path(walFile)); err != nil {
		return err
	}
	s.syncDir()
	f, err := os.OpenFile(s.path(walFile), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(len(tail)), io.SeekStart); err != nil {
		f.Close()
		return err
	}
	s.f = f

	// recompute offsets for the surviving records
	var offs []int64
	var pos int64
	for pos < int64(len(tail)) {
		offs = append(offs, pos)
		pos += recHeader + int64(binary.LittleEndian.Uint32(tail[pos:]))
	}
	if len(offs) > 0 {
		s.first = meta.Index + 1
	}
	s.offsets, s.size = offs, int64(len(tail))
	return nil
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

var _ raft.Store = (*FileStore)(nil)
