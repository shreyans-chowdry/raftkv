package raft

import (
	"fmt"

	"github.com/shreyans-chowdry/raftkv/transport"
)

// Entry is a Raft log entry (see transport.Entry).
type Entry = transport.Entry

// raftLog is the in-memory log. Indexes are absolute: after a snapshot the
// log starts at snapIndex+1 and entries[i].Index == snapIndex+1+i.
//
// Every access goes through the helpers below (term, entryAt, slice, ...);
// nothing else indexes `entries` directly. That is the cheapest defence
// against off-by-one bugs once snapshots shift the offset.
type raftLog struct {
	snapIndex uint64 // lastIncludedIndex of the latest snapshot (0 = none)
	snapTerm  uint64 // lastIncludedTerm
	entries   []Entry
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// firstIndex is the first index that is still stored as an entry.
func (l *raftLog) firstIndex() uint64 { return l.snapIndex + 1 }

// term returns the term of entry i. ok is false if i was compacted into the
// snapshot (other than the snapshot's own last index) or is past the end.
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i == l.snapIndex {
		return l.snapTerm, true
	}
	if i < l.snapIndex || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.snapIndex-1].Term, true
}

// entryAt returns entry i; i must be in [firstIndex, lastIndex].
func (l *raftLog) entryAt(i uint64) Entry {
	if i <= l.snapIndex || i > l.lastIndex() {
		panic(fmt.Sprintf("entryAt(%d) outside [%d,%d]", i, l.firstIndex(), l.lastIndex()))
	}
	return l.entries[i-l.snapIndex-1]
}

// slice returns entries [lo, hi). The result shares memory with the log, so
// callers must not modify it; the log itself never modifies entries in place
// (truncation and compaction re-slice or copy).
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo <= l.snapIndex || hi > l.lastIndex()+1 || lo > hi {
		panic(fmt.Sprintf("slice(%d,%d) outside [%d,%d]", lo, hi, l.firstIndex(), l.lastIndex()+1))
	}
	return l.entries[lo-l.snapIndex-1 : hi-l.snapIndex-1]
}

// batchFrom returns entries starting at lo, at most maxCount of them and
// roughly at most maxBytes of payload (always at least one entry if any exist).
func (l *raftLog) batchFrom(lo uint64, maxCount int, maxBytes int) []Entry {
	if lo > l.lastIndex() {
		return nil
	}
	all := l.slice(lo, l.lastIndex()+1)
	size := 0
	for i, e := range all {
		size += len(e.Data) + 24
		if i > 0 && (i >= maxCount || size > maxBytes) {
			return all[:i]
		}
	}
	if len(all) > maxCount {
		return all[:maxCount]
	}
	return all
}

// truncateFrom deletes entries with index >= i.
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.snapIndex {
		panic(fmt.Sprintf("truncateFrom(%d) would cut into snapshot at %d", i, l.snapIndex))
	}
	if i > l.lastIndex() {
		return
	}
	// Re-slice with capacity clipped so a later append can't overwrite memory
	// that an in-flight message might still reference.
	n := i - l.snapIndex - 1
	l.entries = l.entries[:n:n]
}

func (l *raftLog) append(es ...Entry) {
	for _, e := range es {
		if e.Index != l.lastIndex()+1 {
			panic(fmt.Sprintf("append index %d, expected %d", e.Index, l.lastIndex()+1))
		}
		l.entries = append(l.entries, e)
	}
}

// compactTo discards everything up to and including index i, which becomes
// the snapshot point with term t. Entries after i are kept if present.
func (l *raftLog) compactTo(i, t uint64) {
	if i <= l.snapIndex {
		return
	}
	var rest []Entry
	if i < l.lastIndex() {
		old := l.slice(i+1, l.lastIndex()+1)
		rest = make([]Entry, len(old))
		copy(rest, old)
	}
	l.snapIndex, l.snapTerm, l.entries = i, t, rest
}

// lastIndexOfTerm returns the last index whose entry has term t.
func (l *raftLog) lastIndexOfTerm(t uint64) (uint64, bool) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term == t {
			return l.entries[i].Index, true
		}
		if l.entries[i].Term < t {
			return 0, false
		}
	}
	if l.snapTerm == t && l.snapIndex > 0 {
		return l.snapIndex, true
	}
	return 0, false
}

// firstIndexOfTerm returns the first index (>= firstIndex) of the run of
// entries with term t that ends at or after `at`. Used for ConflictIndex.
func (l *raftLog) firstIndexOfTerm(t uint64, at uint64) uint64 {
	i := at
	for i > l.firstIndex() {
		pt, _ := l.term(i - 1)
		if pt != t {
			break
		}
		i--
	}
	return i
}
