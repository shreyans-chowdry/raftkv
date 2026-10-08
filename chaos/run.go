package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/shreyans-chowdry/raftkv/raft"
)

// RunConfig describes one chaos run. Everything random is derived from Seed.
type RunConfig struct {
	Seed     int64
	Duration time.Duration // fault-injection phase
	Clients  int           // concurrent clerks (default 5)
	Keys     int           // keyspace size (default 5, so ops conflict)
	Nodes    int           // default 5
	// FailureDir receives a Porcupine visualisation for failed runs.
	FailureDir string
}

// Result is the outcome of a run.
type Result struct {
	Seed         int64
	MaxRaftState int64
	ReadIndex    bool
	CheckQuorum  bool
	Ops          int // operations checked
	Gets, Writes int
	Events       int
	Leaders      int    // terms that had a leader
	Linearizable string // "Ok", "Illegal" or "Unknown" (checker timeout)
	Err          error  // any failure (safety or liveness)
	EventLog     string
	Visualized   string // path of the Porcupine HTML, if written
	Elapsed      time.Duration
}

// Variant picks per-seed options so the runs cover snapshots (none, small,
// tiny thresholds), both read paths, and leaders with and without
// check-quorum (without it, deposed leaders linger, which is the hard case
// for reads).
func Variant(seed int64) (maxRaftState int64, readIndex, checkQuorum bool) {
	switch seed % 3 {
	case 0:
		maxRaftState = -1
	case 1:
		maxRaftState = 4000
	case 2:
		maxRaftState = 1000
	}
	return maxRaftState, (seed/3)%2 == 1, (seed/6)%2 == 0
}

// RunOne runs one seeded chaos scenario and checks it.
func RunOne(cfg RunConfig) Result {
	if cfg.Clients == 0 {
		cfg.Clients = 5
	}
	if cfg.Keys == 0 {
		cfg.Keys = 5
	}
	t0 := time.Now()
	res := Result{Seed: cfg.Seed}
	res.MaxRaftState, res.ReadIndex, res.CheckQuorum = Variant(cfg.Seed)
	c, err := NewCluster(Options{N: cfg.Nodes, Seed: cfg.Seed, MaxRaftState: res.MaxRaftState,
		ReadIndex: res.ReadIndex, NoCheckQuorum: !res.CheckQuorum})
	if err != nil {
		res.Err = err
		return res
	}
	defer c.Shutdown()

	elog := NewEventLog()
	hist := NewHistory()
	sched := NewScheduler(c, cfg.Seed, elog)

	stopFaults := make(chan struct{})
	faultsDone := make(chan struct{})
	go func() {
		sched.Run(stopFaults)
		close(faultsDone)
	}()

	// Clients run until told to stop starting new operations; each one then
	// finishes its current operation (bounded by finishCtx).
	stopClients := make(chan struct{})
	finishCtx, cancelFinish := context.WithCancel(context.Background())
	defer cancelFinish()
	var wg sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := c.Clerk(uint64(cfg.Seed)<<8 | uint64(i+1))
			rng := rand.New(rand.NewSource(cfg.Seed*31 + int64(i)))
			for n := 0; ; n++ {
				select {
				case <-stopClients:
					return
				default:
				}
				if i == 0 {
					// Pace the reader so histories stay small enough for
					// Porcupine to search when they are NOT linearizable.
					time.Sleep(time.Duration(1+rng.Intn(4)) * time.Millisecond)
				}
				key := fmt.Sprintf("k%d", rng.Intn(cfg.Keys))
				var in KVInput
				// Client 0 only reads. A reader that sticks to whichever
				// server last answered it is what exposes a deposed leader
				// serving stale reads; mixed clients move on as soon as one
				// of their writes times out.
				switch r := rng.Intn(10); {
				case r < 4 || i == 0:
					in = KVInput{Op: "get", Key: key}
				case r < 7:
					in = KVInput{Op: "put", Key: key, Value: fmt.Sprintf("p%d.%d ", i, n)}
				default:
					in = KVInput{Op: "append", Key: key, Value: fmt.Sprintf("a%d.%d ", i, n)}
				}
				call := hist.Now()
				var out KVOutput
				var err error
				switch in.Op {
				case "get":
					out.Value, err = ck.Get(finishCtx, key)
				case "put":
					err = ck.Put(finishCtx, key, in.Value)
				default:
					err = ck.Append(finishCtx, key, in.Value)
				}
				if err != nil {
					// Only happens when the run is being torn down after a
					// liveness failure. A write we gave up on may still take
					// effect later, so it is recorded as returning "never":
					// Porcupine may then linearize it at any point after its
					// call. An abandoned read tells us nothing; drop it.
					if in.Op != "get" {
						hist.Add(i, in, out, call, 1<<62)
					}
					return
				}
				hist.Add(i, in, out, call, hist.Now())
			}
		}(i)
	}

	time.Sleep(cfg.Duration)
	close(stopFaults)
	<-faultsDone
	sched.Calm()
	close(stopClients)

	// Liveness: with the network calm and every node up, all clients must
	// finish their in-flight operation promptly.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		res.Err = fmt.Errorf("liveness: clients still blocked 20s after the network was healed")
		cancelFinish()
		<-done
	}
	res.Events = elog.Len()
	res.Leaders = c.Checker.Leaders()

	if err := c.Checker.Err(); err != nil && res.Err == nil {
		res.Err = err
	}
	if err := raft.CheckLogMatching(c.AllLogs()); err != nil && res.Err == nil {
		res.Err = err
	}

	ops := hist.Ops()
	res.Ops = len(ops)
	for _, op := range ops {
		if op.Input.(KVInput).Op == "get" {
			res.Gets++
		} else {
			res.Writes++
		}
	}
	result, info := porcupine.CheckOperationsVerbose(KVModel, ops, 60*time.Second)
	res.Linearizable = string(result)
	if result == porcupine.Illegal {
		if res.Err == nil {
			res.Err = fmt.Errorf("history is not linearizable")
		}
		if cfg.FailureDir != "" {
			os.MkdirAll(cfg.FailureDir, 0o755)
			p := filepath.Join(cfg.FailureDir, fmt.Sprintf("seed-%d.html", cfg.Seed))
			if err := porcupine.VisualizePath(KVModel, info, p); err == nil {
				res.Visualized = p
			}
		}
	}
	if result == porcupine.Unknown && res.Err == nil {
		res.Err = fmt.Errorf("porcupine timed out (history too large to check in 60s)")
	}
	if res.Ops < cfg.Clients && res.Err == nil {
		res.Err = fmt.Errorf("liveness: only %d operations completed", res.Ops)
	}
	res.EventLog = elog.String()
	res.Elapsed = time.Since(t0)
	return res
}
