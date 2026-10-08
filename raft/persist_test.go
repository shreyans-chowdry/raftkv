package raft

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shreyans-chowdry/raftkv/transport"
	"github.com/shreyans-chowdry/raftkv/transport/sim"
)

var persistRuns = flag.Int("persistruns", 300, "seeded runs for TestCrashFigure8ManySeeds")

func TestPersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPersister(dir)
	if _, err := p.Load(); err != nil {
		t.Fatal(err)
	}
	p.SaveHardState(3, 2)
	p.AppendEntries([]Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 1, Data: []byte("b")}})
	p.AppendEntries([]Entry{{Index: 3, Term: 2, Data: []byte("c")}})
	p.Truncate(3)
	p.AppendEntries([]Entry{{Index: 3, Term: 3, Data: []byte("C")}})
	p.SaveHardState(4, 0)
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	p.SaveHardState(9, 9) // never synced: a crash loses it
	p.Close()

	p2, _ := OpenPersister(dir)
	st, err := p2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.Term != 4 || st.Vote != 0 {
		t.Fatalf("hard state %d/%d, want 4/0", st.Term, st.Vote)
	}
	if len(st.Entries) != 3 || st.Entries[2].Term != 3 || string(st.Entries[2].Data) != "C" {
		t.Fatalf("entries %+v", st.Entries)
	}
}

// TestTornWrite cuts raft.log in the middle of the last record (as a crash
// during write would) and flips a byte in another copy; recovery must stop at
// the last good record and keep working afterwards.
func TestTornWrite(t *testing.T) {
	for _, mode := range []string{"truncate", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			p, _ := OpenPersister(dir)
			p.Load()
			p.SaveHardState(1, 1)
			p.AppendEntries([]Entry{{Index: 1, Term: 1, Data: []byte("one")}})
			p.Sync()
			good, _ := os.Stat(filepath.Join(dir, logFile))
			p.AppendEntries([]Entry{{Index: 2, Term: 1, Data: []byte("two-two-two")}})
			p.Sync()
			p.Close()

			path := filepath.Join(dir, logFile)
			data, _ := os.ReadFile(path)
			if mode == "truncate" {
				data = data[:good.Size()+10] // mid-record
			} else {
				data[len(data)-3] ^= 0xFF // bad CRC on the last record
			}
			os.WriteFile(path, data, 0o644)

			p2, _ := OpenPersister(dir)
			st, err := p2.Load()
			if err != nil {
				t.Fatal(err)
			}
			if st.Term != 1 || len(st.Entries) != 1 || string(st.Entries[0].Data) != "one" {
				t.Fatalf("recovered %+v", st)
			}
			// The torn tail is gone, so new records land after good ones.
			p2.AppendEntries([]Entry{{Index: 2, Term: 2, Data: []byte("new")}})
			p2.Sync()
			p2.Close()
			p3, _ := OpenPersister(dir)
			st, _ = p3.Load()
			if len(st.Entries) != 2 || string(st.Entries[1].Data) != "new" {
				t.Fatalf("after recovery+append: %+v", st.Entries)
			}
			p3.Close()
		})
	}
}

// TestSnapshotCrashBetweenFiles: crash after the new snapshot is durable but
// before raft.log is rewritten. Load must combine the new snapshot with the
// old log correctly.
func TestSnapshotCrashBetweenFiles(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPersister(dir)
	p.Load()
	p.SaveHardState(2, 1)
	var es []Entry
	for i := uint64(1); i <= 10; i++ {
		es = append(es, Entry{Index: i, Term: 1 + i/6, Data: []byte(fmt.Sprint(i))})
	}
	p.AppendEntries(es)
	p.Sync()
	p.InjectCrashAfterSnapshotFile()
	if err := p.SaveSnapshot(6, es[5].Term, []byte("snap6"), 2, 1, es[6:]); err == nil {
		t.Fatalf("expected injected crash")
	}
	p.Close()

	p2, _ := OpenPersister(dir)
	st, err := p2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.SnapIndex != 6 || string(st.Snapshot) != "snap6" || len(st.Entries) != 4 || st.Entries[0].Index != 7 {
		t.Fatalf("recovered snap %d %q, %d entries", st.SnapIndex, st.Snapshot, len(st.Entries))
	}
	// A complete snapshot then rewrites raft.log.
	if err := p2.SaveSnapshot(8, st.Entries[1].Term, []byte("snap8"), 2, 1, st.Entries[2:]); err != nil {
		t.Fatal(err)
	}
	p2.Close()
	p3, _ := OpenPersister(dir)
	st, _ = p3.Load()
	if st.SnapIndex != 8 || len(st.Entries) != 2 || st.Term != 2 || st.Vote != 1 {
		t.Fatalf("after full snapshot: %+v", st)
	}
	p3.Close()
}

func TestPersistBasic(t *testing.T) {
	h := newHarness(t, 3, 20, nil)
	defer h.cleanup()
	h.one(cmd("11"), 3, true)
	term0 := h.checkTerms()
	for i := 0; i < 3; i++ {
		h.restart(i)
	}
	h.one(cmd("12"), 3, true)
	if term1 := h.checkTerms(); term1 < term0 {
		t.Fatalf("term went backwards after restart: %d -> %d", term0, term1)
	}
	l := h.checkOneLeader()
	h.restart(l)
	h.one(cmd("13"), 3, true)
	l = h.checkOneLeader()
	h.crash(l)
	h.one(cmd("14"), 2, true)
	h.start(l)
	h.wait(4, 3, 0)
	h.crash(0)
	h.crash(1)
	h.crash(2)
	h.start(0)
	h.start(1)
	h.start(2)
	h.one(cmd("15"), 3, true)
}

// TestVoteSurvivesRestart: a node must remember whom it voted for in its
// current term, or it could vote twice and allow two leaders.
func TestVoteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPersister(dir)
	p.Load()
	net := sim.New(1)
	ch := make(chan ApplyMsg, 8)
	n, _ := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Transport: net.Endpoint(1), Storage: p,
		Clock: NewManualClock(), ElectionTicksMin: 1000, ApplyCh: ch})
	cand := net.Endpoint(2)
	cand.Send(1, msgVote(7))
	net.Advance(1)
	waitFor(t, func() bool { return n.Status().Term == 7 })
	net.Advance(1)
	n.Stop()
	p.Close()

	p2, _ := OpenPersister(dir)
	st, _ := p2.Load()
	if st.Term != 7 || st.Vote != 2 {
		t.Fatalf("after restart term=%d vote=%d, want 7/2", st.Term, st.Vote)
	}
	p2.Close()
}

// TestCrashFigure8ManySeeds runs the Figure 8 scenario with crashes instead
// of disconnects (so unsynced state is lost) on an unreliable network, many
// times with different seeds.
func TestCrashFigure8ManySeeds(t *testing.T) {
	runs := *persistRuns
	if testing.Short() {
		runs = 10
	}
	sem := make(chan struct{}, 10)
	var wg sync.WaitGroup
	var failed atomic.Int64
	for s := 1; s <= runs; s++ {
		seed := int64(1000 + s)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ft := &fakeT{name: fmt.Sprintf("seed %d", seed)}
			func() {
				defer func() {
					if r := recover(); r != nil && r != errFakeFatal {
						ft.mu.Lock()
						ft.msg = fmt.Sprintf("panic: %v", r)
						ft.mu.Unlock()
					}
				}()
				crashFigure8(ft, seed)
			}()
			if ft.failed() {
				failed.Add(1)
				t.Errorf("%s: %s", ft.name, ft.msg)
			}
		}()
	}
	wg.Wait()
	t.Logf("%d seeded crash runs, %d failed", runs, failed.Load())
}

func crashFigure8(t testing.TB, seed int64) {
	h := newHarness(t, 5, seed, nil)
	defer h.cleanup()
	rng := rand.New(rand.NewSource(seed))
	h.net.SetLinkConfig(sim.LinkConfig{DropProb: 0.2, MinDelay: 0, MaxDelay: 10, Reorder: true})
	h.one(cmd("s%d", seed), 1, true)
	nup := 5
	for iter := 0; iter < 60; iter++ {
		leader := -1
		for i := 0; i < 5; i++ {
			if h.live(i) {
				if _, _, ok := h.node(i).Start(cmd("x%d", iter)); ok {
					leader = i
				}
			}
		}
		if rng.Intn(100) < 10 {
			time.Sleep(time.Duration(rng.Intn(electionMs()/2)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rng.Intn(13)) * time.Millisecond)
		}
		victim := -1
		if leader != -1 && rng.Intn(100) < 40 {
			victim = leader
		} else if rng.Intn(100) < 30 {
			victim = rng.Intn(5) // any node, often a follower mid-replication
		}
		if victim != -1 && h.live(victim) {
			h.crash(victim)
			nup--
		}
		for nup < 3 {
			s := rng.Intn(5)
			if !h.live(s) {
				h.start(s)
				nup++
			}
		}
	}
	for i := 0; i < 5; i++ {
		if !h.live(i) {
			h.start(i)
		}
	}
	h.net.SetLinkConfig(sim.LinkConfig{})
	h.one(cmd("final%d", seed), 5, true)
}

// TestGroupCommitFsyncs measures fsyncs per committed entry with and without
// group commit, under 16 concurrent proposers.
func TestGroupCommitFsyncs(t *testing.T) {
	measure := func(disable bool) (float64, time.Duration) {
		h := newHarness(t, 3, 30, func(c *Config) { c.DisableGroupCommit = disable })
		defer h.cleanup()
		h.checkOneLeader()
		const total = 400
		start := time.Now()
		before := uint64(0)
		for i := 0; i < 3; i++ {
			before += h.stores[i].SyncCount()
		}
		var wg sync.WaitGroup
		for c := 0; c < 16; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				for j := 0; j < total/16; j++ {
					h.one(cmd("g%d-%d", c, j), 3, true)
				}
			}(c)
		}
		wg.Wait()
		after := uint64(0)
		for i := 0; i < 3; i++ {
			after += h.stores[i].SyncCount()
		}
		return float64(after-before) / float64(total), time.Since(start)
	}
	without, d1 := measure(true)
	with, d2 := measure(false)
	t.Logf("fsyncs per committed entry (sum over 3 nodes): without group commit %.2f (%v), with group commit %.2f (%v)",
		without, d1.Round(time.Millisecond), with, d2.Round(time.Millisecond))
	if with >= without {
		t.Fatalf("group commit did not reduce fsyncs: %.2f vs %.2f", with, without)
	}
}

func msgVote(term uint64) transport.Message {
	return transport.Message{Type: transport.MsgVote, Term: term}
}

// fakeT lets many seeded scenarios run concurrently and report failures
// with their seed without stopping the others.
type fakeT struct {
	testing.TB
	name string
	mu   sync.Mutex
	msg  string
}

var errFakeFatal = fmt.Errorf("fatal")

func (f *fakeT) Fatalf(format string, args ...interface{}) {
	f.mu.Lock()
	if f.msg == "" {
		f.msg = fmt.Sprintf(format, args...)
	}
	f.mu.Unlock()
	panic(errFakeFatal)
}
func (f *fakeT) Fatal(args ...interface{}) { f.Fatalf("%s", fmt.Sprint(args...)) }
func (f *fakeT) Helper()                    {}
func (f *fakeT) Logf(string, ...interface{}) {}
func (f *fakeT) failed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.msg != ""
}
