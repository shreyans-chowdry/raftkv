package raft

import (
	"math/rand"
	"testing"
	"time"

	"github.com/shreyans-chowdry/raftkv/transport"
	"github.com/shreyans-chowdry/raftkv/transport/sim"
)

func TestBasicAgree3(t *testing.T) { testBasicAgree(t, 3) }
func TestBasicAgree5(t *testing.T) { testBasicAgree(t, 5) }

func testBasicAgree(t *testing.T, n int) {
	h := newHarness(t, n, 10, nil)
	defer h.cleanup()
	h.checkOneLeader()
	var last uint64
	for i := 1; i <= 10; i++ {
		idx := h.one(cmd("c%d", i), n, false)
		if idx <= last {
			t.Fatalf("index %d did not increase (prev %d)", idx, last)
		}
		last = idx
	}
}

func TestFollowerRejoins(t *testing.T) {
	h := newHarness(t, 3, 11, nil)
	defer h.cleanup()
	h.one(cmd("a"), 3, false)
	l := h.checkOneLeader()
	f := (l + 1) % 3
	h.disconnect(f)
	// The remaining two still agree.
	h.one(cmd("b"), 2, false)
	h.one(cmd("c"), 2, true)
	time.Sleep(electionWait)
	h.one(cmd("d"), 2, true)
	// Rejoined follower catches up.
	h.connect(f)
	h.one(cmd("e"), 3, true)
	h.one(cmd("f"), 3, true)
}

func TestNoAgreeWithoutMajority(t *testing.T) {
	h := newHarness(t, 5, 12, nil)
	defer h.cleanup()
	h.one(cmd("x"), 5, false)
	l := h.checkOneLeader()
	for k := 1; k <= 3; k++ {
		h.disconnect((l + k) % 5)
	}
	idx, _, ok := h.node(l).Start(cmd("never"))
	if !ok {
		t.Fatalf("leader rejected Start")
	}
	time.Sleep(2 * electionWait)
	if nd, _ := h.nCommitted(idx); nd > 0 {
		t.Fatalf("%d committed without a majority", nd)
	}
	for k := 1; k <= 3; k++ {
		h.connect((l + k) % 5)
	}
	// Agreement resumes once a majority is back. The uncommitted entry may
	// or may not survive; either way the log keeps working.
	h.one(cmd("after"), 5, true)
}

// TestRejoinOverwritesUncommitted: a leader with uncommitted entries is cut
// off, a new leader commits different entries at the same indexes, and when
// the old leader comes back its conflicting entries are replaced.
func TestRejoinOverwritesUncommitted(t *testing.T) {
	h := newHarness(t, 3, 13, nil)
	defer h.cleanup()
	h.one(cmd("101"), 3, true)

	l1 := h.checkOneLeader()
	h.disconnect(l1)
	n := h.node(l1)
	n.Start(cmd("102-lost"))
	n.Start(cmd("103-lost"))
	n.Start(cmd("104-lost"))

	h.one(cmd("103"), 2, true)

	l2 := h.checkOneLeader()
	h.disconnect(l2)
	h.connect(l1)
	h.one(cmd("104"), 2, true)

	h.connect(l2)
	h.one(cmd("105"), 3, true)
	h.checkLogMatching()
	h.mu.Lock()
	defer h.mu.Unlock()
	for idx, c := range h.cmds {
		if len(c) > 5 && string(c[len(c)-5:]) == "-lost" {
			t.Fatalf("uncommitted entry %q from the cut-off leader was applied at %d", c, idx)
		}
	}
}

// TestBackupFast checks that a leader can bring a follower with a long
// conflicting suffix back in sync quickly (fast backup by conflict term).
func TestBackupFast(t *testing.T) {
	h := newHarness(t, 5, 14, nil)
	defer h.cleanup()
	h.one(cmd("start"), 5, true)

	// Leader and one follower get cut off with 50 uncommitted entries.
	l1 := h.checkOneLeader()
	h.disconnect((l1 + 2) % 5)
	h.disconnect((l1 + 3) % 5)
	h.disconnect((l1 + 4) % 5)
	for i := 0; i < 50; i++ {
		h.node(l1).Start(cmd("lost-%d", i))
	}
	time.Sleep(electionWait / 2)
	h.disconnect(l1)
	h.disconnect((l1 + 1) % 5)

	// The other three commit 50 entries.
	h.connect((l1 + 2) % 5)
	h.connect((l1 + 3) % 5)
	h.connect((l1 + 4) % 5)
	for i := 0; i < 50; i++ {
		h.one(cmd("ok-%d", i), 3, true)
	}

	// Now another leader with an even longer divergent suffix.
	l2 := h.checkOneLeader()
	other := (l1 + 2) % 5
	if l2 == other {
		other = (l2 + 1) % 5
	}
	h.disconnect(other)
	for i := 0; i < 50; i++ {
		h.node(l2).Start(cmd("lost2-%d", i))
	}
	time.Sleep(electionWait / 2)

	for i := 0; i < 5; i++ {
		h.disconnect(i)
	}
	h.connect(l1)
	h.connect((l1 + 1) % 5)
	h.connect(other)
	for i := 0; i < 50; i++ {
		h.one(cmd("ok2-%d", i), 3, true)
	}
	for i := 0; i < 5; i++ {
		h.connect(i)
	}
	start := time.Now()
	h.one(cmd("final"), 5, true)
	t.Logf("all 5 in sync %v after healing", time.Since(start).Round(time.Millisecond))
}

// TestFigure8Unreliable is the scenario from Figure 8 of the paper, on a
// network that drops 20% of messages and reorders them: leaders keep getting
// cut off mid-replication, so entries from old terms sit on some servers
// uncommitted. The invariant checker verifies nobody ever commits an entry a
// later leader overwrites.
func TestFigure8Unreliable(t *testing.T) {
	h := newHarness(t, 5, 15, nil)
	defer h.cleanup()
	h.net.SetLinkConfig(sim.LinkConfig{DropProb: 0.2, MinDelay: 0, MaxDelay: 15, Reorder: true})
	rng := rand.New(rand.NewSource(15))
	h.one(cmd("seed"), 1, true)
	nup := 5
	for iter := 0; iter < 200; iter++ {
		if iter == 150 {
			h.net.SetLinkConfig(sim.LinkConfig{DropProb: 0.2, MinDelay: 0, MaxDelay: 3, Reorder: true})
		}
		leader := -1
		for i := 0; i < 5; i++ {
			if h.live(i) {
				if _, _, ok := h.node(i).Start(cmd("f8-%d", iter)); ok {
					leader = i
				}
			}
		}
		if rng.Intn(1000) < 100 {
			time.Sleep(time.Duration(rng.Intn(electionMs()/2)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rng.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rng.Intn(1000) < 500 {
			h.disconnect(leader)
			nup--
		}
		if nup < 3 {
			s := rng.Intn(5)
			if !h.live(s) {
				h.connect(s)
				nup++
			}
		}
	}
	for i := 0; i < 5; i++ {
		h.connect(i)
	}
	h.net.SetLinkConfig(sim.LinkConfig{})
	h.one(cmd("final"), 5, true)
}

func electionMs() int { return int(electionWait / time.Millisecond) }

// TestUnreliableAgree: many concurrent proposals on a lossy, reordering
// network all get committed in the same order everywhere.
func TestUnreliableAgree(t *testing.T) {
	h := newHarness(t, 5, 16, nil)
	defer h.cleanup()
	h.net.SetLinkConfig(sim.LinkConfig{DropProb: 0.2, MinDelay: 0, MaxDelay: 10, Reorder: true})
	done := make(chan struct{})
	for c := 0; c < 4; c++ {
		go func(c int) {
			for j := 0; j < 12; j++ {
				h.one(cmd("u-%d-%d", c, j), 1, true)
			}
			done <- struct{}{}
		}(c)
	}
	for c := 0; c < 4; c++ {
		<-done
	}
	h.net.SetLinkConfig(sim.LinkConfig{})
	h.one(cmd("end"), 5, true)
}

// TestFigure8CommitRule checks the commit rule directly, with fake peers: a
// leader in term 4 whose log holds an entry from term 2 must not commit it
// just because a majority stores it. It commits only once an entry from term
// 4 is on a majority (and then the term-2 entry commits with it).
func TestFigure8CommitRule(t *testing.T) {
	dir := t.TempDir()
	st, _ := OpenPersister(dir)
	st.Load()
	st.SaveHardState(3, 0)
	st.AppendEntries([]Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 2, Data: []byte("b")}})
	st.Sync()
	st.Close()
	st, _ = OpenPersister(dir)

	clk := NewManualClock()
	net := sim.New(1)
	peers := []NodeID{1, 2, 3, 4, 5}
	fake := map[NodeID]transport.Transport{}
	for _, p := range peers[1:] {
		fake[p] = net.Endpoint(p)
	}
	ch := make(chan ApplyMsg, 16)
	n, err := NewNode(Config{ID: 1, Peers: peers, Transport: net.Endpoint(1), Storage: st, Clock: clk,
		TickInterval: time.Millisecond, ElectionTicksMin: 5, ElectionTicksMax: 5, HeartbeatTicks: 1000,
		Seed: 1, ApplyCh: ch})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	advanceTicks(t, n, clk, 5) // campaign in term 4
	waitFor(t, func() bool { return n.Status().Term == 4 })
	for _, p := range []NodeID{2, 3} {
		fake[p].Send(1, transport.Message{Type: transport.MsgVoteResp, Term: 4, Granted: true})
	}
	net.Advance(1)
	waitFor(t, func() bool { _, l := n.GetState(); return l })
	if s := n.Status(); s.LastIndex != 3 {
		t.Fatalf("leader should have appended a no-op at 3, lastIndex=%d", s.LastIndex)
	}

	ack := func(match uint64) {
		for _, p := range []NodeID{2, 3} {
			fake[p].Send(1, transport.Message{Type: transport.MsgAppResp, Term: 4, Success: true, MatchIndex: match})
		}
		net.Advance(1)
		// Let the node process both acks (a status call round-trips the loop).
		waitFor(t, func() bool {
			s := n.Status()
			return s.Match != nil && s.Match[2] >= match && s.Match[3] >= match
		})
	}
	ack(2)
	if c := n.Status().CommitIndex; c != 0 {
		t.Fatalf("committed up to %d counting replicas of a term-2 entry; Figure 8 says wait", c)
	}
	ack(3)
	if c := n.Status().CommitIndex; c != 3 {
		t.Fatalf("commit index %d after the term-4 entry reached a majority, want 3", c)
	}
}
