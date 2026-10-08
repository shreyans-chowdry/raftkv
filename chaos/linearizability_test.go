package chaos

import (
	"flag"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	flagSeed     = flag.Int64("seed", 0, "replay this one seed (0 = run -runs seeds)")
	flagRuns     = flag.Int("runs", 8, "number of seeded runs")
	flagFirst    = flag.Int64("first-seed", 1, "first seed when running many")
	flagDuration = flag.Duration("duration", 5*time.Second, "fault-injection time per run")
	flagParallel = flag.Int("parallel-runs", 0, "runs at once (0 = 2 x GOMAXPROCS)")
)

// TestChaos runs seeded chaos scenarios: 5 nodes, 5 concurrent clients on 5
// keys, faults every 200-800ms, then a calm period in which every client must
// finish. Each history is checked for linearizability with Porcupine; Raft's
// invariant checker and log matching run as well.
//
//	go test ./chaos -run 'TestChaos$' -runs=500 -duration=5s   # many seeds
//	go test ./chaos -run 'TestChaos$' -seed=137 -v              # replay one
//
// Replaying a seed repeats the same fault schedule, workload and network
// randomness. Goroutine scheduling is not controlled, so the exact
// interleaving can differ; in practice a failing seed fails again within a
// few replays.
func TestChaos(t *testing.T) {
	dur := *flagDuration
	if testing.Short() && *flagSeed == 0 {
		dur = 2 * time.Second
	}
	if *flagSeed != 0 {
		res := RunOne(RunConfig{Seed: *flagSeed, Duration: dur, FailureDir: "failures"})
		t.Logf("seed %d (maxRaftState=%d readIndex=%v checkQuorum=%v): %d ops (%d gets, %d writes), %d fault events, %d leaders, porcupine=%s, %v",
			res.Seed, res.MaxRaftState, res.ReadIndex, res.CheckQuorum, res.Ops, res.Gets, res.Writes, res.Events, res.Leaders, res.Linearizable, res.Elapsed.Round(time.Millisecond))
		t.Logf("fault schedule:\n%s", res.EventLog)
		if res.Err != nil {
			if res.Visualized != "" {
				t.Logf("history visualisation: %s", res.Visualized)
			}
			t.Fatalf("seed %d: %v", res.Seed, res.Err)
		}
		return
	}

	runs := *flagRuns
	par := *flagParallel
	if par <= 0 {
		par = 2 * runtime.GOMAXPROCS(0)
	}
	start := time.Now()
	results := make([]Result, runs)
	sem := make(chan struct{}, par)
	var wg sync.WaitGroup
	var mu sync.Mutex
	finished := 0
	for i := 0; i < runs; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			seed := *flagFirst + int64(i)
			r := RunOne(RunConfig{Seed: seed, Duration: dur, FailureDir: "failures"})
			results[i] = r
			mu.Lock()
			finished++
			if r.Err != nil {
				t.Logf("FAIL seed %d: %v", seed, r.Err)
			}
			if testing.Verbose() && (finished%50 == 0 || finished == runs) {
				t.Logf("progress: %d/%d runs done (%v)", finished, runs, time.Since(start).Round(time.Second))
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	var failedSeeds []string
	ops, gets, writes, leaders, events := 0, 0, 0, 0, 0
	byVariant := map[string]int{}
	for _, r := range results {
		ops += r.Ops
		gets += r.Gets
		writes += r.Writes
		leaders += r.Leaders
		events += r.Events
		byVariant[fmt.Sprintf("maxRaftState=%-5d readIndex=%-5v checkQuorum=%v", r.MaxRaftState, r.ReadIndex, r.CheckQuorum)]++
		if r.Err != nil {
			failedSeeds = append(failedSeeds, fmt.Sprint(r.Seed))
		}
	}
	var variants []string
	for v, n := range byVariant {
		variants = append(variants, fmt.Sprintf("  %s  %d runs", v, n))
	}
	sort.Strings(variants)
	summary := fmt.Sprintf(`chaos summary
  runs:               %d (seeds %d..%d, %v fault injection each, %d in parallel)
  failures:           %d
  operations checked: %d (%d gets, %d puts/appends)
  fault events:       %d
  leaders elected:    %d
  wall time:          %v
variants:
%s`, runs, *flagFirst, *flagFirst+int64(runs)-1, dur, par, len(failedSeeds), ops, gets, writes,
		events, leaders, time.Since(start).Round(time.Second), strings.Join(variants, "\n"))
	if len(failedSeeds) > 0 {
		summary += "\nfailed seeds: " + strings.Join(failedSeeds, " ")
	}
	t.Log("\n" + summary)
	if len(failedSeeds) > 0 {
		t.Fatalf("%d of %d runs failed; replay with: go test ./chaos -run 'TestChaos$' -seed=N -v", len(failedSeeds), runs)
	}
}

// TestCheckerCatchesStaleRead makes sure the Porcupine model actually
// rejects a non-linearizable history (a read that returns an overwritten
// value after the overwrite completed), so a passing chaos run means
// something.
func TestCheckerCatchesStaleRead(t *testing.T) {
	h := NewHistory()
	h.Add(0, KVInput{Op: "put", Key: "x", Value: "1"}, KVOutput{}, 0, 10)
	h.Add(1, KVInput{Op: "put", Key: "x", Value: "2"}, KVOutput{}, 20, 30)
	h.Add(2, KVInput{Op: "get", Key: "x"}, KVOutput{Value: "1"}, 40, 50)
	if res := checkOps(h); res != "Illegal" {
		t.Fatalf("stale read accepted: %s", res)
	}
	h2 := NewHistory()
	h2.Add(0, KVInput{Op: "put", Key: "x", Value: "1"}, KVOutput{}, 0, 10)
	h2.Add(1, KVInput{Op: "put", Key: "x", Value: "2"}, KVOutput{}, 20, 45) // overlaps the read
	h2.Add(2, KVInput{Op: "get", Key: "x"}, KVOutput{Value: "1"}, 40, 50)
	if res := checkOps(h2); res != "Ok" {
		t.Fatalf("legal history rejected: %s", res)
	}
}
