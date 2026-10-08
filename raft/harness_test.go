package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shreyans-chowdry/raftkv/transport"
	"github.com/shreyans-chowdry/raftkv/transport/sim"
)

// Test timing: 10ms ticks, 150-300ms election timeout, 50ms heartbeats.
// Shorter than production so tests run fast, but still well above message
// latency under the race detector.
const (
	testTick     = 10 * time.Millisecond
	testElectMin = 15
	testElectMax = 30
	testHB       = 5
	electionWait = time.Duration(testElectMax) * testTick
)

type harness struct {
	t       testing.TB
	n       int
	seed    int64
	net     *sim.Network
	checker *InvariantChecker
	tweak   func(*Config)

	mu        sync.Mutex
	nodes     []*Node
	stores    []*Persister
	dirs      []string
	up        []bool
	connected []bool
	applied   []map[uint64][]byte // per node, survives restarts
	lastApp   []uint64            // per node incarnation, for ordering checks
	cmds      map[uint64][]byte   // index -> command applied by anyone
	errs      []string
	snapEvery int
	wg        sync.WaitGroup
}

func newHarness(t testing.TB, n int, seed int64, tweak func(*Config)) *harness {
	h := &harness{
		t: t, n: n, seed: seed, tweak: tweak,
		net:       sim.New(seed),
		checker:   NewInvariantChecker(),
		nodes:     make([]*Node, n),
		stores:    make([]*Persister, n),
		dirs:      make([]string, n),
		up:        make([]bool, n),
		connected: make([]bool, n),
		applied:   make([]map[uint64][]byte, n),
		lastApp:   make([]uint64, n),
		cmds:      map[uint64][]byte{},
	}
	for i := 0; i < n; i++ {
		d, err := os.MkdirTemp("", fmt.Sprintf("raft-test-n%d-", i+1))
		if err != nil {
			t.Fatal(err)
		}
		h.dirs[i] = d
		h.applied[i] = map[uint64][]byte{}
		h.net.Endpoint(transport.NodeID(i + 1))
	}
	h.net.Start(time.Millisecond)
	for i := 0; i < n; i++ {
		h.start(i)
	}
	return h
}

func (h *harness) peers() []NodeID {
	var ps []NodeID
	for i := 1; i <= h.n; i++ {
		ps = append(ps, NodeID(i))
	}
	return ps
}

// start boots node i from whatever is on its disk.
func (h *harness) start(i int) {
	id := NodeID(i + 1)
	st, err := OpenPersister(h.dirs[i])
	if err != nil {
		h.t.Fatal(err)
	}
	applyCh := make(chan ApplyMsg, 256)
	cfg := Config{
		ID: id, Peers: h.peers(), Transport: h.net.Restart(id), Storage: st,
		TickInterval: testTick, ElectionTicksMin: testElectMin, ElectionTicksMax: testElectMax,
		HeartbeatTicks: testHB, Seed: h.seed*100 + int64(i) + 1,
		ApplyCh: applyCh, Observer: h.checker,
	}
	if h.tweak != nil {
		h.tweak(&cfg)
	}
	node, err := NewNode(cfg)
	if err != nil {
		h.t.Fatalf("starting node %d: %v", id, err)
	}
	h.mu.Lock()
	h.nodes[i], h.stores[i], h.up[i], h.connected[i] = node, st, true, true
	h.lastApp[i] = 0
	h.mu.Unlock()
	h.wg.Add(1)
	go h.consume(i, node, applyCh)
}

// consume checks every applied message: in order, no gaps, and the same
// command at each index on every node.
func (h *harness) consume(i int, node *Node, ch <-chan ApplyMsg) {
	defer h.wg.Done()
	for m := range ch {
		h.mu.Lock()
		if m.SnapshotValid {
			if m.SnapshotIndex < h.lastApp[i] {
				h.errorf("node %d: snapshot %d moves backwards from %d", i+1, m.SnapshotIndex, h.lastApp[i])
			}
			h.lastApp[i] = m.SnapshotIndex
			h.mu.Unlock()
			continue
		}
		if m.CommandIndex != h.lastApp[i]+1 {
			h.errorf("node %d: applied index %d, expected %d", i+1, m.CommandIndex, h.lastApp[i]+1)
		}
		h.lastApp[i] = m.CommandIndex
		if prev, ok := h.cmds[m.CommandIndex]; ok && !bytes.Equal(prev, m.Command) {
			h.errorf("node %d applied %q at %d, another node applied %q", i+1, m.Command, m.CommandIndex, prev)
		}
		h.cmds[m.CommandIndex] = m.Command
		h.applied[i][m.CommandIndex] = m.Command
		snap := h.snapEvery > 0 && m.CommandIndex%uint64(h.snapEvery) == 0
		h.mu.Unlock()
		if snap {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, m.CommandIndex)
			node.Snapshot(m.CommandIndex, b)
		}
	}
}

func (h *harness) errorf(format string, args ...interface{}) {
	if len(h.errs) < 10 {
		h.errs = append(h.errs, fmt.Sprintf(format, args...))
	}
}

func (h *harness) crash(i int) {
	h.mu.Lock()
	node, st := h.nodes[i], h.stores[i]
	h.up[i] = false
	h.mu.Unlock()
	if node == nil {
		return
	}
	h.net.Crash(NodeID(i + 1))
	node.Stop()
	st.Close() // drops anything not fsync'd
}

func (h *harness) restart(i int) {
	h.crash(i)
	h.start(i)
}

func (h *harness) disconnect(i int) {
	h.net.Disconnect(NodeID(i + 1))
	h.mu.Lock()
	h.connected[i] = false
	h.mu.Unlock()
}

func (h *harness) connect(i int) {
	h.net.Reconnect(NodeID(i + 1))
	h.mu.Lock()
	h.connected[i] = true
	h.mu.Unlock()
}

func (h *harness) live(i int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.up[i] && h.connected[i]
}

func (h *harness) node(i int) *Node {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[i]
}

// cleanup stops everything and reports any invariant violation.
func (h *harness) cleanup() {
	for i := 0; i < h.n; i++ {
		if h.live(i) || h.up[i] {
			h.checkLogMatching()
			break
		}
	}
	for i := 0; i < h.n; i++ {
		h.crash(i)
	}
	h.net.Stop()
	h.wg.Wait()
	for _, d := range h.dirs {
		os.RemoveAll(d)
	}
	if err := h.checker.Err(); err != nil {
		h.t.Fatalf("seed %d: %v", h.seed, err)
	}
	if len(h.errs) > 0 {
		h.t.Fatalf("seed %d: apply errors: %v", h.seed, h.errs)
	}
}

func (h *harness) checkLogMatching() {
	var logs []LogSnapshot
	for i := 0; i < h.n; i++ {
		h.mu.Lock()
		node, up := h.nodes[i], h.up[i]
		h.mu.Unlock()
		if !up {
			continue
		}
		if l, ok := node.DebugLog(); ok {
			logs = append(logs, l)
		}
	}
	if err := CheckLogMatching(logs); err != nil {
		h.t.Fatalf("seed %d: %v", h.seed, err)
	}
}

// checkOneLeader waits for exactly one leader among the live nodes (in the
// newest term) and returns its index.
func (h *harness) checkOneLeader() int {
	for iter := 0; iter < 250; iter++ {
		time.Sleep(20 * time.Millisecond)
		leaders := map[uint64][]int{}
		for i := 0; i < h.n; i++ {
			if !h.live(i) {
				continue
			}
			if term, lead := h.node(i).GetState(); lead {
				leaders[term] = append(leaders[term], i)
			}
		}
		var last uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				h.t.Fatalf("seed %d: term %d has %d leaders", h.seed, term, len(ls))
			}
			if term > last {
				last = term
			}
		}
		if len(leaders) != 0 {
			return leaders[last][0]
		}
	}
	h.t.Fatalf("seed %d: expected one leader, got none", h.seed)
	return -1
}

func (h *harness) checkNoLeader() {
	for i := 0; i < h.n; i++ {
		if !h.live(i) {
			continue
		}
		if _, lead := h.node(i).GetState(); lead {
			h.t.Fatalf("seed %d: node %d is leader but should not be", h.seed, i+1)
		}
	}
}

func (h *harness) checkTerms() uint64 {
	var term uint64
	for i := 0; i < h.n; i++ {
		if !h.live(i) {
			continue
		}
		t, _ := h.node(i).GetState()
		if term == 0 {
			term = t
		} else if t != term {
			h.t.Fatalf("seed %d: servers disagree on term (%d vs %d)", h.seed, term, t)
		}
	}
	return term
}

// nCommitted returns how many nodes have applied index and the command.
func (h *harness) nCommitted(index uint64) (int, []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	var cmd []byte
	for i := 0; i < h.n; i++ {
		if c, ok := h.applied[i][index]; ok {
			if count > 0 && !bytes.Equal(c, cmd) {
				h.t.Fatalf("seed %d: index %d committed as %q and %q", h.seed, index, cmd, c)
			}
			count++
			cmd = c
		}
	}
	return count, cmd
}

// wait waits until at least n nodes applied index; returns the command.
func (h *harness) wait(index uint64, n int, startTerm uint64) []byte {
	to := 10 * time.Millisecond
	for iter := 0; iter < 30; iter++ {
		nd, _ := h.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > 0 {
			for i := 0; i < h.n; i++ {
				if h.up[i] {
					if t, _ := h.node(i).GetState(); t > startTerm {
						return nil // someone moved on; caller can't expect success
					}
				}
			}
		}
	}
	nd, cmd := h.nCommitted(index)
	if nd < n {
		h.t.Fatalf("seed %d: only %d decided for index %d; wanted %d", h.seed, nd, index, n)
	}
	return cmd
}

// one submits cmd to whichever node is leader and waits until `expected`
// nodes have applied it. With retry, it resubmits if leadership changes.
func (h *harness) one(cmd []byte, expected int, retry bool) uint64 {
	t0 := time.Now()
	start := 0
	for time.Since(t0) < 15*time.Second {
		index := uint64(0)
		for k := 0; k < h.n; k++ {
			start = (start + 1) % h.n
			if !h.live(start) {
				continue
			}
			if idx, _, ok := h.node(start).Start(cmd); ok {
				index = idx
				break
			}
		}
		if index != 0 {
			t1 := time.Now()
			for time.Since(t1) < 3*time.Second {
				nd, c := h.nCommitted(index)
				if nd > 0 && nd >= expected && bytes.Equal(c, cmd) {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				h.t.Fatalf("seed %d: one(%q) failed to reach agreement", h.seed, cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	h.t.Fatalf("seed %d: one(%q) failed to reach agreement in 15s", h.seed, cmd)
	return 0
}

func cmd(s string, args ...interface{}) []byte { return []byte(fmt.Sprintf(s, args...)) }
