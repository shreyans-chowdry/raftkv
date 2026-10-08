package raft

import (
	"bytes"
	"fmt"
	"sync"
)

// LogView is read-only access to a node's log, handed to an Observer.
type LogView interface {
	// Term returns the term of entry i, if the log still holds it.
	Term(i uint64) (uint64, bool)
	LastIndex() uint64
	// SnapshotIndex is the last index covered by the snapshot; entries up
	// to it are no longer individually stored.
	SnapshotIndex() uint64
}

func (l *raftLog) Term(i uint64) (uint64, bool) { return l.term(i) }
func (l *raftLog) LastIndex() uint64            { return l.lastIndex() }
func (l *raftLog) SnapshotIndex() uint64        { return l.snapIndex }

// Observer is notified from inside a node's event loop. Calls must be quick
// and must not call back into the node.
type Observer interface {
	// LeaderElected is called when node id becomes leader of term.
	LeaderElected(id NodeID, term uint64, log LogView)
	// Committed is called for every index a node learns is committed.
	Committed(id NodeID, index, term uint64)
}

// InvariantChecker is an Observer that every node in a test cluster reports
// to. It checks, continuously while the cluster runs:
//
//   - Election Safety: at most one leader per term.
//   - Leader Completeness: a new leader's log contains every entry that any
//     node has seen committed (same index, same term).
//   - Commit agreement (State Machine Safety at the log level): no two nodes
//     ever commit different terms at the same index.
//
// Log Matching is checked separately by CheckLogMatching, which needs whole
// logs. Violations are recorded, not panicked, so a test can report them
// together with the seed that produced them.
type InvariantChecker struct {
	mu         sync.Mutex
	leaders    map[uint64]NodeID // term -> leader
	committed  map[uint64]uint64 // index -> term
	maxIndex   uint64
	violations []string
}

// NewInvariantChecker returns an empty checker.
func NewInvariantChecker() *InvariantChecker {
	return &InvariantChecker{leaders: map[uint64]NodeID{}, committed: map[uint64]uint64{}}
}

func (c *InvariantChecker) fail(format string, args ...interface{}) {
	if len(c.violations) < 20 {
		c.violations = append(c.violations, fmt.Sprintf(format, args...))
	}
}

// LeaderElected implements Observer.
func (c *InvariantChecker) LeaderElected(id NodeID, term uint64, lv LogView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if other, ok := c.leaders[term]; ok && other != id {
		c.fail("election safety: term %d has two leaders, %d and %d", term, other, id)
	}
	c.leaders[term] = id
	snap := lv.SnapshotIndex()
	for idx, t := range c.committed {
		if idx <= snap {
			continue // compacted entries were committed, so they're present
		}
		lt, ok := lv.Term(idx)
		if !ok || lt != t {
			c.fail("leader completeness: new leader %d (term %d) lacks committed entry %d (term %d); has term %d ok=%v",
				id, term, idx, t, lt, ok)
		}
	}
}

// Committed implements Observer.
func (c *InvariantChecker) Committed(id NodeID, index, term uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.committed[index]; ok && t != term {
		c.fail("commit agreement: node %d committed index %d with term %d, previously term %d", id, index, term, t)
	}
	c.committed[index] = term
	if index > c.maxIndex {
		c.maxIndex = index
	}
}

// Leaders returns how many distinct terms had a leader.
func (c *InvariantChecker) Leaders() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.leaders)
}

// Err returns nil, or an error describing every violation seen.
func (c *InvariantChecker) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.violations) == 0 {
		return nil
	}
	return fmt.Errorf("%d invariant violation(s): %v", len(c.violations), c.violations)
}

// CheckLogMatching verifies the Log Matching property across logs: if two
// logs contain an entry with the same index and term, the logs are identical
// in all entries up through that index. It compares every pair over the
// range both still store.
func CheckLogMatching(logs []LogSnapshot) error {
	for a := 0; a < len(logs); a++ {
		for b := a + 1; b < len(logs); b++ {
			if err := matchPair(logs[a], logs[b]); err != nil {
				return err
			}
		}
	}
	return nil
}

func matchPair(x, y LogSnapshot) error {
	at := func(l LogSnapshot, i uint64) (Entry, bool) {
		if i <= l.SnapIndex || i > l.SnapIndex+uint64(len(l.Entries)) {
			return Entry{}, false
		}
		return l.Entries[i-l.SnapIndex-1], true
	}
	lo := x.SnapIndex
	if y.SnapIndex > lo {
		lo = y.SnapIndex
	}
	hiX := x.SnapIndex + uint64(len(x.Entries))
	hiY := y.SnapIndex + uint64(len(y.Entries))
	hi := hiX
	if hiY < hi {
		hi = hiY
	}
	// Find the highest index where both agree on the term; everything at or
	// below it (that both store) must be identical.
	var top uint64
	for i := hi; i > lo; i-- {
		ex, _ := at(x, i)
		ey, _ := at(y, i)
		if ex.Term == ey.Term {
			top = i
			break
		}
	}
	for i := lo + 1; i <= top; i++ {
		ex, _ := at(x, i)
		ey, _ := at(y, i)
		if ex.Term != ey.Term || ex.Type != ey.Type || !bytes.Equal(ex.Data, ey.Data) {
			return fmt.Errorf("log matching: nodes %d and %d agree at index %d but differ at %d (terms %d vs %d)",
				x.ID, y.ID, top, i, ex.Term, ey.Term)
		}
	}
	// Committed prefixes must agree too.
	c := x.Commit
	if y.Commit < c {
		c = y.Commit
	}
	if c > hi {
		c = hi
	}
	for i := lo + 1; i <= c; i++ {
		ex, _ := at(x, i)
		ey, _ := at(y, i)
		if ex.Term != ey.Term {
			return fmt.Errorf("log matching: nodes %d and %d differ at committed index %d", x.ID, y.ID, i)
		}
	}
	return nil
}
