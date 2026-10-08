package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/shreyans-chowdry/raftkv/transport"
)

// Persister stores Raft's durable state in a directory:
//
//	raft.log      append-only file of CRC-framed records (term/vote changes,
//	              appended entries, truncations)
//	snapshot.bin  the latest snapshot (index, term, service data), CRC-framed
//
// Record framing is [len uint32][crc32c(payload) uint32][payload], little
// endian. On startup the log is replayed; the first record that is short or
// fails its CRC marks a torn write from a crash, and the file is truncated
// there. Anything after that point was never fsync'd, so no node ever relied
// on it.
//
// Writes are buffered in memory until Sync, which does one write + one fsync
// for everything buffered. That is what makes group commit possible: the node
// buffers several updates and syncs once. A crash (Close without Sync, or
// just dropping the Persister) loses exactly the unsynced buffer, which is
// what a real power cut would lose.
//
// Persister is used by one goroutine (the node's event loop) except for Size
// and SyncCount, which are safe to call concurrently.
type Persister struct {
	dir  string
	mu   sync.Mutex // guards f and buf against Close racing with the loop
	f    *os.File
	buf  []byte
	size atomic.Int64 // bytes in raft.log including unsynced buffer
	// syncs counts fsyncs of raft.log (for measuring group commit).
	syncs atomic.Uint64

	crashAfterSnapshotFile bool
}

const (
	logFile  = "raft.log"
	snapFile = "snapshot.bin"

	recHardState byte = 1
	recEntries   byte = 2
	recTruncate  byte = 3
	recBase      byte = 4
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// PersistentState is what Load recovers.
type PersistentState struct {
	Term      uint64
	Vote      transport.NodeID
	SnapIndex uint64
	SnapTerm  uint64
	Snapshot  []byte
	Entries   []Entry // entries after SnapIndex, in order
}

// OpenPersister opens (creating if needed) the state in dir.
func OpenPersister(dir string) (*Persister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &Persister{dir: dir}
	return p, nil
}

// Load reads the persisted state and opens raft.log for appending. It must be
// called once, before any write.
func (p *Persister) Load() (PersistentState, error) {
	var st PersistentState
	path := filepath.Join(p.dir, logFile)
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return st, err
	}
	var lg raftLog
	good := 0
	for off := 0; ; {
		payload, n, ok := readFrame(data[off:])
		if !ok {
			break
		}
		if err := applyRecord(&st, &lg, payload); err != nil {
			return st, fmt.Errorf("raft.log record at offset %d: %w", off, err)
		}
		off += n
		good = off
	}
	created := errors.Is(err, os.ErrNotExist)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return st, err
	}
	if good < len(data) {
		// Torn or corrupt tail: cut it off so new records follow good ones.
		if err := f.Truncate(int64(good)); err != nil {
			f.Close()
			return st, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return st, err
		}
	}
	if _, err := f.Seek(int64(good), io.SeekStart); err != nil {
		f.Close()
		return st, err
	}
	if created {
		syncDir(p.dir)
	}
	p.f = f
	p.size.Store(int64(good))

	// Snapshot file.
	sdata, err := os.ReadFile(filepath.Join(p.dir, snapFile))
	if err == nil {
		payload, _, ok := readFrame(sdata)
		if !ok || len(payload) < 16 {
			return st, errors.New("snapshot.bin is corrupt")
		}
		idx := binary.LittleEndian.Uint64(payload[0:])
		term := binary.LittleEndian.Uint64(payload[8:])
		st.Snapshot = append([]byte(nil), payload[16:]...)
		if idx < lg.snapIndex {
			return st, fmt.Errorf("snapshot.bin at %d is older than raft.log base %d", idx, lg.snapIndex)
		}
		// The snapshot may be newer than the log's base if we crashed after
		// writing the snapshot but before rewriting raft.log.
		if t, ok := lg.term(idx); ok && t == term {
			lg.compactTo(idx, term)
		} else {
			lg = raftLog{snapIndex: idx, snapTerm: term}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return st, err
	} else if lg.snapIndex > 0 {
		return st, errors.New("raft.log has a snapshot base but snapshot.bin is missing")
	}
	st.SnapIndex, st.SnapTerm, st.Entries = lg.snapIndex, lg.snapTerm, lg.entries
	return st, nil
}

func applyRecord(st *PersistentState, lg *raftLog, b []byte) error {
	if len(b) == 0 {
		return errors.New("empty record")
	}
	d := decoder{b: b[1:]}
	switch b[0] {
	case recHardState:
		st.Term = d.u64()
		st.Vote = transport.NodeID(d.u64())
	case recBase:
		idx, term := d.u64(), d.u64()
		*lg = raftLog{snapIndex: idx, snapTerm: term}
	case recTruncate:
		from := d.u64()
		if from <= lg.snapIndex {
			*lg = raftLog{snapIndex: lg.snapIndex, snapTerm: lg.snapTerm}
		} else {
			lg.truncateFrom(from)
		}
	case recEntries:
		n := int(d.u32())
		for i := 0; i < n && d.err == nil; i++ {
			e := Entry{Index: d.u64(), Term: d.u64(), Type: transport.EntryType(d.u8())}
			e.Data = d.bytes()
			if e.Index <= lg.snapIndex {
				continue
			}
			if e.Index <= lg.lastIndex() {
				lg.truncateFrom(e.Index)
			}
			if e.Index != lg.lastIndex()+1 {
				return fmt.Errorf("gap: entry %d after %d", e.Index, lg.lastIndex())
			}
			lg.append(e)
		}
	default:
		return fmt.Errorf("unknown record type %d", b[0])
	}
	return d.err
}

// readFrame parses one [len][crc][payload] frame. ok is false for a short or
// corrupt frame.
func readFrame(b []byte) (payload []byte, n int, ok bool) {
	if len(b) < 8 {
		return nil, 0, false
	}
	l := int(binary.LittleEndian.Uint32(b))
	crc := binary.LittleEndian.Uint32(b[4:])
	if l > len(b)-8 {
		return nil, 0, false
	}
	payload = b[8 : 8+l]
	if crc32.Checksum(payload, crcTable) != crc {
		return nil, 0, false
	}
	return payload, 8 + l, true
}

func appendFrame(dst, payload []byte) []byte {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, crcTable))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

func (p *Persister) addRecord(payload []byte) {
	before := len(p.buf)
	p.buf = appendFrame(p.buf, payload)
	p.size.Add(int64(len(p.buf) - before))
}

// SaveHardState buffers a term/vote update.
func (p *Persister) SaveHardState(term uint64, vote transport.NodeID) {
	var e encoder
	e.u8(recHardState)
	e.u64(term)
	e.u64(uint64(vote))
	p.mu.Lock()
	p.addRecord(e.b)
	p.mu.Unlock()
}

// AppendEntries buffers appended entries.
func (p *Persister) AppendEntries(es []Entry) {
	if len(es) == 0 {
		return
	}
	var e encoder
	e.u8(recEntries)
	e.u32(uint32(len(es)))
	for _, x := range es {
		e.u64(x.Index)
		e.u64(x.Term)
		e.u8(uint8(x.Type))
		e.bytes(x.Data)
	}
	p.mu.Lock()
	p.addRecord(e.b)
	p.mu.Unlock()
}

// Truncate buffers the deletion of all entries with index >= from.
func (p *Persister) Truncate(from uint64) {
	var e encoder
	e.u8(recTruncate)
	e.u64(from)
	p.mu.Lock()
	p.addRecord(e.b)
	p.mu.Unlock()
}

// Pending reports whether there are buffered, unsynced records.
func (p *Persister) Pending() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buf) > 0
}

// Sync writes all buffered records and fsyncs raft.log. It does nothing if
// nothing is buffered.
func (p *Persister) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.buf) == 0 {
		return nil
	}
	if p.f == nil {
		return errors.New("persister closed")
	}
	if _, err := p.f.Write(p.buf); err != nil {
		return err
	}
	if err := p.f.Sync(); err != nil {
		return err
	}
	p.syncs.Add(1)
	p.buf = p.buf[:0]
	return nil
}

// SaveSnapshot durably installs a snapshot and rewrites raft.log so it only
// contains the hard state and the entries after the snapshot. Order matters
// for crash safety: the snapshot file is made durable first (write temp,
// fsync, rename, fsync dir), then raft.log is replaced the same way. A crash
// between the two leaves a new snapshot next to an old log, which Load
// handles by compacting the old log to the snapshot.
func (p *Persister) SaveSnapshot(index, term uint64, data []byte, hsTerm uint64, vote transport.NodeID, rest []Entry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f == nil {
		return errors.New("persister closed")
	}
	payload := make([]byte, 16, 16+len(data))
	binary.LittleEndian.PutUint64(payload[0:], index)
	binary.LittleEndian.PutUint64(payload[8:], term)
	payload = append(payload, data...)
	if err := writeAtomic(p.dir, snapFile, appendFrame(nil, payload)); err != nil {
		return err
	}
	if p.crashAfterSnapshotFile {
		return errors.New("injected crash after snapshot file")
	}

	var buf []byte
	var e encoder
	e.u8(recBase)
	e.u64(index)
	e.u64(term)
	buf = appendFrame(buf, e.b)
	e = encoder{}
	e.u8(recHardState)
	e.u64(hsTerm)
	e.u64(uint64(vote))
	buf = appendFrame(buf, e.b)
	if len(rest) > 0 {
		e = encoder{}
		e.u8(recEntries)
		e.u32(uint32(len(rest)))
		for _, x := range rest {
			e.u64(x.Index)
			e.u64(x.Term)
			e.u8(uint8(x.Type))
			e.bytes(x.Data)
		}
		buf = appendFrame(buf, e.b)
	}
	if err := writeAtomic(p.dir, logFile, buf); err != nil {
		return err
	}
	p.f.Close()
	f, err := os.OpenFile(filepath.Join(p.dir, logFile), os.O_RDWR, 0o644)
	if err != nil {
		p.f = nil
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		p.f = nil
		return err
	}
	p.f = f
	p.buf = p.buf[:0]
	p.size.Store(int64(len(buf)))
	p.syncs.Add(1)
	return nil
}

// InjectCrashAfterSnapshotFile is a test hook: the next SaveSnapshot stops
// right after the snapshot file is durable and returns an error, as if the
// process died before rewriting raft.log.
func (p *Persister) InjectCrashAfterSnapshotFile() {
	p.mu.Lock()
	p.crashAfterSnapshotFile = true
	p.mu.Unlock()
}

// Size returns the size of raft.log in bytes, including buffered records.
// The KV layer compares it with maxRaftState to decide when to snapshot.
func (p *Persister) Size() int64 { return p.size.Load() }

// SyncCount returns how many fsyncs of raft.log have happened.
func (p *Persister) SyncCount() uint64 { return p.syncs.Load() }

// Close closes the file WITHOUT syncing buffered records, which simulates a
// crash. Call Sync first for a clean shutdown.
func (p *Persister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.buf = nil
	if p.f == nil {
		return nil
	}
	err := p.f.Close()
	p.f = nil
	return err
}

// Dir returns the directory holding the state.
func (p *Persister) Dir() string { return p.dir }

func writeAtomic(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// encoder/decoder: a minimal little-endian binary format. Hand-rolled rather
// than gob because gob puts type information at the start of a stream, and
// this file is a sequence of independent records that must each be readable
// on their own (after a torn write, after a rewrite).
type encoder struct{ b []byte }

func (e *encoder) u8(v uint8)   { e.b = append(e.b, v) }
func (e *encoder) u32(v uint32) { e.b = binary.LittleEndian.AppendUint32(e.b, v) }
func (e *encoder) u64(v uint64) { e.b = binary.LittleEndian.AppendUint64(e.b, v) }
func (e *encoder) bytes(v []byte) {
	e.u32(uint32(len(v)))
	e.b = append(e.b, v...)
}

type decoder struct {
	b   []byte
	err error
}

var errShort = errors.New("record too short")

func (d *decoder) need(n int) bool {
	if d.err != nil || len(d.b) < n {
		d.err = errShort
		return false
	}
	return true
}
func (d *decoder) u8() uint8 {
	if !d.need(1) {
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}
func (d *decoder) u32() uint32 {
	if !d.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(d.b)
	d.b = d.b[4:]
	return v
}
func (d *decoder) u64() uint64 {
	if !d.need(8) {
		return 0
	}
	v := binary.LittleEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}
func (d *decoder) bytes() []byte {
	n := int(d.u32())
	if !d.need(n) {
		return nil
	}
	v := append([]byte(nil), d.b[:n]...)
	d.b = d.b[n:]
	return v
}
