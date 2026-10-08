package raft

import (
	"flag"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shreyans-chowdry/raftkv/transport"
	"github.com/shreyans-chowdry/raftkv/transport/sim"
)

var electionRuns = flag.Int("elections", 1000, "seeded runs for TestElectionSafetyManySeeds")

func TestInitialElection3(t *testing.T) { testInitialElection(t, 3) }
func TestInitialElection5(t *testing.T) { testInitialElection(t, 5) }

func testInitialElection(t *testing.T, n int) {
	h := newHarness(t, n, 1, nil)
	defer h.cleanup()
	h.checkOneLeader()
	term1 := h.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, expected >= 1", term1)
	}
	// With no failures, the leader keeps its job and the term stays put.
	time.Sleep(2 * electionWait)
	if term2 := h.checkTerms(); term2 != term1 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}
	h.checkOneLeader()
}

func TestReElection(t *testing.T) {
	h := newHarness(t, 3, 2, nil)
	defer h.cleanup()
	l1 := h.checkOneLeader()

	// Leader disconnects: a new one is elected (within ~1s).
	start := time.Now()
	h.disconnect(l1)
	l2 := h.checkOneLeader()
	if l2 == l1 {
		t.Fatalf("old leader still counted as leader")
	}
	t.Logf("re-election after leader loss took %v", time.Since(start).Round(10*time.Millisecond))

	// Old leader rejoins: it must step down; still exactly one leader.
	h.connect(l1)
	h.checkOneLeader()
	time.Sleep(electionWait)
	if term, lead := h.node(l1).GetState(); lead {
		cur := h.checkTerms()
		if term < cur {
			t.Fatalf("stale leader %d still leading in old term %d", l1+1, term)
		}
	}

	// No quorum: no leader.
	l3 := h.checkOneLeader()
	h.disconnect(l3)
	h.disconnect((l3 + 1) % 3)
	time.Sleep(2 * electionWait)
	h.checkNoLeader()

	// Quorum back: leader.
	h.connect((l3 + 1) % 3)
	h.checkOneLeader()
	h.connect(l3)
	h.checkOneLeader()
}

func TestNoLeaderWithoutMajority5(t *testing.T) {
	h := newHarness(t, 5, 3, nil)
	defer h.cleanup()
	h.checkOneLeader()
	for i := 0; i < 3; i++ {
		h.disconnect(i)
	}
	time.Sleep(3 * electionWait)
	// Nodes 4 and 5 can't elect anyone. (A node in the minority that was
	// leader before steps down via check-quorum.)
	for i := 3; i < 5; i++ {
		if _, lead := h.node(i).GetState(); lead {
			t.Fatalf("node %d is leader without a majority", i+1)
		}
	}
	for i := 0; i < 3; i++ {
		h.connect(i)
	}
	h.checkOneLeader()
}

// TestElectionTimerBounds drives a single node with a manual clock: it must
// not start an election before ElectionTicksMin ticks and must have started
// one by ElectionTicksMax.
func TestElectionTimerBounds(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		dir := t.TempDir()
		st, _ := OpenPersister(dir)
		clk := NewManualClock()
		net := sim.New(seed)
		applyCh := make(chan ApplyMsg, 16)
		// A 3-node config where the other two never answer: the node can
		// become a candidate but never leader, so we can watch its term.
		n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Transport: net.Endpoint(1),
			Storage: st, Clock: clk, TickInterval: time.Millisecond,
			ElectionTicksMin: 10, ElectionTicksMax: 20, HeartbeatTicks: 2, Seed: seed, ApplyCh: applyCh})
		if err != nil {
			t.Fatal(err)
		}
		advanceTicks(t, n, clk, 9)
		if term, _ := n.GetState(); term != 0 {
			t.Fatalf("seed %d: election started after 9 ticks (min is 10)", seed)
		}
		advanceTicks(t, n, clk, 11)
		if term, _ := n.GetState(); term != 1 {
			t.Fatalf("seed %d: no election after 20 ticks (term %d)", seed, term)
		}
		n.Stop()
		st.Close()
	}
}

// advanceTicks advances the manual clock one tick at a time and waits until
// the node has processed each one.
func advanceTicks(t *testing.T, n *Node, clk *ManualClock, k int) {
	t.Helper()
	for i := 0; i < k; i++ {
		before := n.Status().Ticks
		clk.Advance(time.Millisecond)
		deadline := time.Now().Add(2 * time.Second)
		for n.Status().Ticks == before {
			if time.Now().After(deadline) {
				t.Fatalf("node did not process tick")
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// TestTimerNotResetByRejectedCandidate checks the reset rule: a follower that
// keeps receiving RequestVote from a candidate whose log is behind (and so
// keeps refusing it) must still time out and campaign itself.
func TestTimerNotResetByRejectedCandidate(t *testing.T) {
	dir := t.TempDir()
	st, _ := OpenPersister(dir)
	// Give node 1 a log entry so a candidate with an empty log is behind it.
	st.Load()
	st.SaveHardState(1, 0)
	st.AppendEntries([]Entry{{Index: 1, Term: 1, Data: []byte("x")}})
	st.Sync()
	st.Close()
	st, _ = OpenPersister(dir)

	clk := NewManualClock()
	net := sim.New(1)
	peer2 := net.Endpoint(2)
	net.Endpoint(3)
	applyCh := make(chan ApplyMsg, 16)
	n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Transport: net.Endpoint(1),
		Storage: st, Clock: clk, TickInterval: time.Millisecond,
		ElectionTicksMin: 10, ElectionTicksMax: 20, HeartbeatTicks: 2, Seed: 5, ApplyCh: applyCh})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	campaigned := false
	for tick := 0; tick < 25 && !campaigned; tick++ {
		// Stale-log candidate, ever higher term, every tick.
		peer2.Send(1, transport.Message{Type: transport.MsgVote, Term: uint64(10 + tick)})
		net.Advance(1)
		waitFor(t, func() bool { return n.Status().Term >= uint64(10+tick) })
		advanceTicks(t, n, clk, 1)
		net.Advance(1)
		for {
			select {
			case m := <-peer2.Recv():
				if m.Type == transport.MsgVote && m.From == 1 {
					campaigned = true
				}
				if m.Type == transport.MsgVoteResp && m.Granted {
					t.Fatalf("node 1 voted for a candidate with a stale log")
				}
				continue
			default:
			}
			break
		}
	}
	if !campaigned {
		t.Fatalf("follower never campaigned: its timer was reset by vote requests it rejected")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met in 2s")
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// TestElectionSafetyManySeeds runs many short seeded scenarios with random
// partitions, message loss and reordering, and checks with the invariant
// checker that no term ever has two leaders.
func TestElectionSafetyManySeeds(t *testing.T) {
	runs := *electionRuns
	if testing.Short() {
		runs = 50
	}
	var leaders, failures atomic.Int64
	sem := make(chan struct{}, 24)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed []string
	for s := 1; s <= runs; s++ {
		seed := int64(s)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			n, err := electionChaosRun(seed)
			leaders.Add(int64(n))
			if err != nil {
				failures.Add(1)
				mu.Lock()
				failed = append(failed, err.Error())
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("%d seeded runs, %d leaders elected in total, %d failures", runs, leaders.Load(), failures.Load())
	if len(failed) > 0 {
		t.Fatalf("failures: %v", failed)
	}
}

// electionChaosRun runs one 5-node cluster for ~1.2s while partitioning it
// randomly, and returns the number of terms that had a leader.
func electionChaosRun(seed int64) (int, error) {
	rng := rand.New(rand.NewSource(seed))
	net := sim.New(seed)
	net.SetLinkConfig(sim.LinkConfig{DropProb: 0.1, MinDelay: 0, MaxDelay: 5, Reorder: true})
	checker := NewInvariantChecker()
	var nodes []*Node
	var dirs []string
	peers := []NodeID{1, 2, 3, 4, 5}
	for _, id := range peers {
		d, _ := os.MkdirTemp("", "elect")
		dirs = append(dirs, d)
		st, _ := OpenPersister(d)
		ch := make(chan ApplyMsg, 1024)
		go func() {
			for range ch {
			}
		}()
		n, err := NewNode(Config{ID: id, Peers: peers, Transport: net.Endpoint(id), Storage: st,
			TickInterval: testTick, ElectionTicksMin: 8, ElectionTicksMax: 16, HeartbeatTicks: 3,
			Seed: seed*10 + int64(id), ApplyCh: ch, Observer: checker})
		if err != nil {
			return 0, err
		}
		nodes = append(nodes, n)
	}
	net.Start(time.Millisecond)
	for step := 0; step < 10; step++ {
		time.Sleep(time.Duration(80+rng.Intn(80)) * time.Millisecond)
		switch rng.Intn(3) {
		case 0:
			net.Heal()
		default:
			perm := rng.Perm(5)
			cut := 1 + rng.Intn(4)
			var a, b []NodeID
			for i, p := range perm {
				if i < cut {
					a = append(a, NodeID(p+1))
				} else {
					b = append(b, NodeID(p+1))
				}
			}
			net.Partition(a, b)
		}
	}
	for _, n := range nodes {
		n.Stop()
	}
	net.Stop()
	for _, d := range dirs {
		os.RemoveAll(d)
	}
	return checker.Leaders(), checker.Err()
}
