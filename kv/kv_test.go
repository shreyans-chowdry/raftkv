package kv_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shreyans-chowdry/raftkv/chaos"
	"github.com/shreyans-chowdry/raftkv/raft"
	"github.com/shreyans-chowdry/raftkv/transport/sim"
)

func newCluster(t *testing.T, opt chaos.Options) *chaos.Cluster {
	t.Helper()
	c, err := chaos.NewCluster(opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := raft.CheckLogMatching(c.AllLogs()); err != nil {
			t.Errorf("seed %d: %v", opt.Seed, err)
		}
		c.Shutdown()
		if err := c.Checker.Err(); err != nil {
			t.Errorf("seed %d: %v", opt.Seed, err)
		}
	})
	return c
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestBasic(t *testing.T) {
	c := newCluster(t, chaos.Options{Seed: 1})
	ck := c.Clerk(1)
	ctx := ctxT(t, 20 * time.Second)
	if err := ck.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	ck.Append(ctx, "a", "2")
	ck.Append(ctx, "b", "x")
	if v, _ := ck.Get(ctx, "a"); v != "12" {
		t.Fatalf("a=%q", v)
	}
	if v, _ := ck.Get(ctx, "b"); v != "x" {
		t.Fatalf("b=%q", v)
	}
	if v, _ := ck.Get(ctx, "missing"); v != "" {
		t.Fatalf("missing=%q", v)
	}
}

// faults selects what goes wrong while clients run.
type faults struct {
	unreliable bool // 10% message loss + reordering inside the cluster
	partitions bool // random partitions (leader changes)
	crashes    bool // crash and restart nodes
	clientLoss bool // lose client requests and replies
}

// runClients has nclients clerks append "x <client> <n> y" to their own key
// for the given duration while faults are injected, then checks that every
// key holds exactly that client's appends, each exactly once, in order.
// That catches both lost writes and duplicated retries.
func runClients(t *testing.T, opt chaos.Options, nclients int, dur time.Duration, f faults) {
	c := newCluster(t, opt)
	rng := rand.New(rand.NewSource(opt.Seed))
	if f.unreliable {
		c.SetLink(sim.LinkConfig{DropProb: 0.1, MaxDelay: 10, Reorder: true})
	}
	if f.clientLoss {
		c.SetClientFaults(0.1, 0.1)
	}
	stop := make(chan struct{})
	counts := make([]int, nclients)
	var wg sync.WaitGroup
	errs := make(chan error, nclients)
	for i := 0; i < nclients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := c.Clerk(uint64(i + 1))
			key := fmt.Sprintf("k%d", i)
			ctx := ctxT(t, dur + 60*time.Second)
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := ck.Append(ctx, key, fmt.Sprintf("x %d %d y", i, j)); err != nil {
					errs <- err
					return
				}
				counts[i] = j + 1
				if j%5 == 0 {
					v, err := ck.Get(ctx, key)
					if err != nil {
						errs <- err
						return
					}
					if err := checkAppends(v, i, j+1); err != nil {
						errs <- fmt.Errorf("mid-run: %v", err)
						return
					}
				}
			}
		}(i)
	}

	end := time.Now().Add(dur)
	ids := c.IDs()
	for time.Now().Before(end) {
		time.Sleep(time.Duration(200+rng.Intn(300)) * time.Millisecond)
		if f.partitions {
			perm := rng.Perm(len(ids))
			cut := len(ids)/2 + 1 // majority side
			var a, b []raft.NodeID
			for k, p := range perm {
				if k < cut {
					a = append(a, ids[p])
				} else {
					b = append(b, ids[p])
				}
			}
			if rng.Intn(3) == 0 {
				c.Heal()
			} else {
				c.Partition(a, b)
			}
		}
		if f.crashes {
			id := ids[rng.Intn(len(ids))]
			if c.Up(id) && c.NumUp() > len(ids)/2+1 {
				c.Crash(id)
			} else {
				for _, x := range ids {
					c.Restart(x)
				}
			}
		}
	}
	c.Heal()
	c.SetLink(sim.LinkConfig{})
	c.SetClientFaults(0, 0)
	for _, x := range ids {
		c.Restart(x)
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("seed %d: %v", opt.Seed, err)
	}
	ck := c.Clerk(999)
	total := 0
	for i := 0; i < nclients; i++ {
		v, err := ck.Get(ctxT(t, 30*time.Second), fmt.Sprintf("k%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if err := checkAppends(v, i, counts[i]); err != nil {
			t.Fatalf("seed %d: final: %v", opt.Seed, err)
		}
		total += counts[i]
	}
	t.Logf("%d appends by %d clients, all present exactly once", total, nclients)
	if total < nclients {
		t.Fatalf("clients made no progress")
	}
}

// checkAppends verifies v is exactly "x i 0 y" "x i 1 y" ... "x i n-1 y".
func checkAppends(v string, client, n int) error {
	var want strings.Builder
	for j := 0; j < n; j++ {
		fmt.Fprintf(&want, "x %d %d y", client, j)
	}
	if v != want.String() {
		return fmt.Errorf("client %d: after %d appends got %q", client, n, trunc(v))
	}
	return nil
}

func trunc(s string) string {
	if len(s) > 200 {
		return s[:100] + "..." + s[len(s)-100:]
	}
	return s
}

func TestOneClient(t *testing.T) {
	runClients(t, chaos.Options{Seed: 2}, 1, 2*time.Second, faults{})
}

func TestManyClients(t *testing.T) {
	runClients(t, chaos.Options{Seed: 3}, 5, 2*time.Second, faults{})
}

func TestUnreliable(t *testing.T) {
	runClients(t, chaos.Options{Seed: 4}, 5, 3*time.Second, faults{unreliable: true, clientLoss: true})
}

func TestPartitions(t *testing.T) {
	runClients(t, chaos.Options{Seed: 5}, 5, 4*time.Second, faults{partitions: true})
}

func TestCrashes(t *testing.T) {
	runClients(t, chaos.Options{Seed: 6}, 5, 4*time.Second, faults{crashes: true})
}

func TestEverything(t *testing.T) {
	runClients(t, chaos.Options{Seed: 7}, 5, 5*time.Second,
		faults{unreliable: true, partitions: true, crashes: true, clientLoss: true})
}

func TestEverythingReadIndex(t *testing.T) {
	runClients(t, chaos.Options{Seed: 8, ReadIndex: true}, 5, 5*time.Second,
		faults{unreliable: true, partitions: true, crashes: true, clientLoss: true})
}

// TestDuplicateAppendAppliedOnce drops half of all replies (never requests),
// so the clerk keeps retrying requests the servers already executed. Every
// append must still land exactly once.
func TestDuplicateAppendAppliedOnce(t *testing.T) {
	c := newCluster(t, chaos.Options{Seed: 9})
	c.SetClientFaults(0, 0.5)
	ck := c.Clerk(1)
	ctx := ctxT(t, 60 * time.Second)
	for j := 0; j < 30; j++ {
		if err := ck.Append(ctx, "k", fmt.Sprintf("x 0 %d y", j)); err != nil {
			t.Fatal(err)
		}
	}
	c.SetClientFaults(0, 0)
	v, _ := c.Clerk(2).Get(ctx, "k")
	if err := checkAppends(v, 0, 30); err != nil {
		t.Fatal(err)
	}
}

// --- M5: snapshots ---------------------------------------------------------

func TestSnapshotsBoundLog(t *testing.T) {
	const maxState = 2000
	c := newCluster(t, chaos.Options{Seed: 10, MaxRaftState: maxState})
	ck := c.Clerk(1)
	ctx := ctxT(t, 60 * time.Second)
	biggest := int64(0)
	for j := 0; j < 400; j++ {
		ck.Append(ctx, fmt.Sprintf("k%d", j%10), "0123456789")
		for _, id := range c.IDs() {
			if st := c.Store(id); st != nil && st.Size() > biggest {
				biggest = st.Size()
			}
		}
	}
	t.Logf("largest raft.log seen: %d bytes (limit %d)", biggest, maxState)
	// A follower may receive one batch past the limit before its server
	// snapshots, so allow some slack, but nothing like unbounded growth
	// (400 appends without snapshots would be ~25 KB).
	if biggest > 3*maxState {
		t.Fatalf("raft state grew to %d bytes with maxRaftState %d", biggest, maxState)
	}
}

func TestInstallSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, chaos.Options{Seed: 11, MaxRaftState: 1000})
	ck := c.Clerk(1)
	ctx := ctxT(t, 60 * time.Second)
	ck.Put(ctx, "a", "start")
	leader, _ := c.Leader()
	lagger := raft.NodeID(1)
	if lagger == leader {
		lagger = 2
	}
	c.Isolate(lagger)
	for j := 0; j < 1000; j++ {
		ck.Append(ctx, fmt.Sprintf("k%d", j%20), "v")
	}
	before := c.Node(lagger).Status()
	c.Heal()
	// The lagger's log ends far before the leader's compacted start, so
	// only InstallSnapshot can bring it up to date.
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := c.Node(lagger).Status()
		ls := c.Node(mustLeader(t, c)).Status()
		if st.SnapIndex > before.LastIndex && st.LastApplied >= ls.CommitIndex-5 {
			t.Logf("lagger went from lastIndex %d to snapshot %d, applied %d (leader commit %d)",
				before.LastIndex, st.SnapIndex, st.LastApplied, ls.CommitIndex)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lagger did not catch up: %+v vs leader %+v", st, ls)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if v, _ := ck.Get(ctx, "k0"); v != strings.Repeat("v", 50) {
		t.Fatalf("k0=%q", v)
	}
}

func mustLeader(t *testing.T, c *chaos.Cluster) raft.NodeID {
	for i := 0; i < 200; i++ {
		if id, ok := c.Leader(); ok {
			return id
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader")
	return 0
}

func TestSnapshotsWithFaults(t *testing.T) {
	runClients(t, chaos.Options{Seed: 12, MaxRaftState: 1000}, 5, 5*time.Second,
		faults{unreliable: true, partitions: true, crashes: true, clientLoss: true})
}

func TestSnapshotsWithCrashesReadIndex(t *testing.T) {
	runClients(t, chaos.Options{Seed: 13, MaxRaftState: 1000, ReadIndex: true}, 5, 4*time.Second,
		faults{crashes: true})
}
