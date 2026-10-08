// Command raftkv-bench is a closed-loop load generator for a RaftKV cluster.
//
// N clients each run a loop: send one request, wait for the reply, send the
// next. The mix is 50% Put / 50% Get over a keyspace of --keys keys with
// --value-size byte values. After --warmup it measures for --duration and
// prints throughput and client-observed latency percentiles (exact: every
// latency is kept and sorted), then appends a row to --csv.
//
//	raftkv-bench --servers 1=localhost:7001,... --concurrency 1,8,32,64 \
//	    --warmup 10s --duration 60s --label final --csv benchmarks/results/final.csv
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shreyans-chowdry/raftkv/kv"
	"github.com/shreyans-chowdry/raftkv/raft"
	grpct "github.com/shreyans-chowdry/raftkv/transport/grpc"
)

func main() {
	var (
		servers   = flag.String("servers", "1=localhost:7001,2=localhost:7002,3=localhost:7003,4=localhost:7004,5=localhost:7005", "cluster")
		concFlag  = flag.String("concurrency", "1,8,32,64", "comma-separated client counts to run")
		warmup    = flag.Duration("warmup", 10*time.Second, "warm-up time per level (not measured)")
		duration  = flag.Duration("duration", 60*time.Second, "measured time per level")
		keys      = flag.Int("keys", 10000, "keyspace size")
		valueSize = flag.Int("value-size", 64, "bytes per value")
		putFrac   = flag.Float64("put-fraction", 0.5, "fraction of operations that are Puts")
		label     = flag.String("label", "run", "label written to the CSV")
		csvPath   = flag.String("csv", "", "append results to this CSV file")
		timeline  = flag.String("timeline", "", "write completed ops per 100ms of the measured window to this CSV")
	)
	flag.Parse()
	addrs, err := grpct.ParsePeers(*servers)
	if err != nil {
		log.Fatal(err)
	}
	var levels []int
	for _, s := range strings.Split(*concFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n <= 0 {
			log.Fatalf("bad concurrency %q", s)
		}
		levels = append(levels, n)
	}
	value := strings.Repeat("v", *valueSize)

	fmt.Printf("%-10s %6s %10s %9s %9s %9s %9s %8s\n", "label", "conc", "ops/s", "p50 ms", "p99 ms", "p99.9 ms", "max ms", "errors")
	for _, conc := range levels {
		r := runLevel(addrs, conc, *warmup, *duration, *keys, value, *putFrac, *timeline)
		fmt.Printf("%-10s %6d %10.0f %9.2f %9.2f %9.2f %9.2f %8d\n", *label, conc, r.opsPerSec,
			ms(r.p50), ms(r.p99), ms(r.p999), ms(r.max), r.errors)
		if *csvPath != "" {
			appendCSV(*csvPath, *label, conc, r)
		}
	}
}

type result struct {
	ops                 int
	errors              int64
	opsPerSec           float64
	p50, p99, p999, max time.Duration
}

func runLevel(addrs map[raft.NodeID]string, conc int, warmup, dur time.Duration, keys int, value string, putFrac float64, timeline string) result {
	conns, closeAll, err := grpct.DialCluster(addrs)
	if err != nil {
		log.Fatal(err)
	}
	defer closeAll()

	var measuring atomic.Bool
	var completed atomic.Int64 // ops completed during measurement, for the timeline
	stop := make(chan struct{})
	lat := make([][]time.Duration, conc)
	var errs atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := kv.NewClerk(conns)
			rng := rand.New(rand.NewSource(int64(i) + time.Now().UnixNano()))
			ctx := context.Background()
			local := make([]time.Duration, 0, 1<<16)
			for {
				select {
				case <-stop:
					lat[i] = local
					return
				default:
				}
				key := "key" + strconv.Itoa(rng.Intn(keys))
				start := time.Now()
				var err error
				if rng.Float64() < putFrac {
					err = ck.Put(ctx, key, value)
				} else {
					_, err = ck.Get(ctx, key)
				}
				d := time.Since(start)
				if measuring.Load() {
					if err != nil {
						errs.Add(1)
					} else {
						local = append(local, d)
						completed.Add(1)
					}
				}
			}
		}(i)
	}
	time.Sleep(warmup)
	measuring.Store(true)
	t0 := time.Now()
	if timeline != "" {
		// Sample the completion counter every 100ms; the fault-tolerance
		// run uses this to see the dip when nodes are killed.
		f, err := os.Create(timeline)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintln(f, "t_sec,ops_per_sec")
		prev := int64(0)
		for tick := time.NewTicker(100 * time.Millisecond); time.Since(t0) < dur; {
			<-tick.C
			c := completed.Load()
			fmt.Fprintf(f, "%.1f,%d\n", time.Since(t0).Seconds(), (c-prev)*10)
			prev = c
		}
		f.Close()
	} else {
		time.Sleep(dur)
	}
	measuring.Store(false)
	elapsed := time.Since(t0)
	close(stop)
	wg.Wait()

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	r := result{ops: len(all), errors: errs.Load(), opsPerSec: float64(len(all)) / elapsed.Seconds()}
	if len(all) > 0 {
		r.p50 = pct(all, 0.50)
		r.p99 = pct(all, 0.99)
		r.p999 = pct(all, 0.999)
		r.max = all[len(all)-1]
	}
	return r
}

// pct returns the q-quantile of sorted durations (nearest rank).
func pct(sorted []time.Duration, q float64) time.Duration {
	i := int(q*float64(len(sorted))+0.5) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func appendCSV(path, label string, conc int, r result) {
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if statErr != nil {
		w.Write([]string{"label", "concurrency", "ops_per_sec", "p50_ms", "p99_ms", "p999_ms", "max_ms", "ops", "errors", "time"})
	}
	w.Write([]string{label, strconv.Itoa(conc), fmt.Sprintf("%.1f", r.opsPerSec),
		fmt.Sprintf("%.3f", ms(r.p50)), fmt.Sprintf("%.3f", ms(r.p99)), fmt.Sprintf("%.3f", ms(r.p999)),
		fmt.Sprintf("%.3f", ms(r.max)), strconv.Itoa(r.ops), strconv.FormatInt(r.errors, 10),
		time.Now().UTC().Format(time.RFC3339)})
	w.Flush()
}
