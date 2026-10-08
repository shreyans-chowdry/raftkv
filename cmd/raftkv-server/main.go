// Command raftkv-server runs one RaftKV node: Raft over gRPC, the KV client
// API on the same port, and an HTTP status/admin endpoint.
//
//	raftkv-server --id 1 --peers 1=localhost:7001,2=localhost:7002,3=localhost:7003 \
//	    --listen :7001 --data-dir data/n1 --status-port 8001
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/shreyans-chowdry/raftkv/kv"
	"github.com/shreyans-chowdry/raftkv/raft"
	grpct "github.com/shreyans-chowdry/raftkv/transport/grpc"
)

func main() {
	var (
		id           = flag.Uint64("id", 0, "this node's ID (1..N)")
		peersFlag    = flag.String("peers", "", "all nodes: 1=host:port,2=host:port,... (including this one)")
		listen       = flag.String("listen", "", "gRPC listen address (default: this node's address from --peers)")
		dataDir      = flag.String("data-dir", "", "directory for raft.log and snapshot.bin")
		statusPort   = flag.Int("status-port", 0, "HTTP port for /status and /admin (0 = off)")
		tick         = flag.Duration("tick", 25*time.Millisecond, "Raft tick length")
		electMin     = flag.Int("election-ticks-min", 12, "election timeout lower bound, in ticks")
		electMax     = flag.Int("election-ticks-max", 24, "election timeout upper bound, in ticks")
		hbTicks      = flag.Int("heartbeat-ticks", 3, "heartbeat interval, in ticks")
		maxRaftState = flag.Int64("max-raft-state", 8<<20, "snapshot when raft.log exceeds this many bytes (<=0: never)")
		readIndex    = flag.Bool("read-index", true, "serve Gets with ReadIndex instead of the log")
		batching     = flag.Bool("batching", true, "pack many entries into one AppendEntries")
		inflight     = flag.Int("max-inflight", 64, "pipelining window per follower (1 = no pipelining)")
		groupCommit  = flag.Bool("group-commit", true, "one fsync per event-loop iteration instead of per update")
		verbose      = flag.Bool("v", false, "Raft debug logging")
	)
	flag.Parse()
	peers, err := grpct.ParsePeers(*peersFlag)
	if err != nil {
		log.Fatal(err)
	}
	me := raft.NodeID(*id)
	if _, ok := peers[me]; !ok {
		log.Fatalf("--id %d is not in --peers", *id)
	}
	if *dataDir == "" {
		log.Fatal("--data-dir is required")
	}
	if *listen == "" {
		*listen = peers[me]
	}
	logger := log.New(os.Stderr, fmt.Sprintf("n%d ", me), log.LstdFlags|log.Lmicroseconds)

	store, err := raft.OpenPersister(*dataDir)
	if err != nil {
		logger.Fatal(err)
	}
	tr := grpct.New(me, peers)
	var ids []raft.NodeID
	for p := range peers {
		ids = append(ids, p)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	applyCh := make(chan raft.ApplyMsg, 4096)
	cfg := raft.Config{
		ID: me, Peers: ids, Transport: tr, Storage: store,
		TickInterval: *tick, ElectionTicksMin: *electMin, ElectionTicksMax: *electMax, HeartbeatTicks: *hbTicks,
		MaxInflight: *inflight, DisableBatching: !*batching, DisableGroupCommit: !*groupCommit,
		ApplyCh: applyCh,
	}
	if *verbose {
		cfg.Logger = logger
	}
	rf, err := raft.NewNode(cfg)
	if err != nil {
		logger.Fatal(err)
	}
	srv := kv.NewServer(rf, applyCh, kv.Config{MaxRaftState: *maxRaftState, ReadIndex: *readIndex})

	gs := grpc.NewServer(grpct.ServerOptions()...)
	tr.Register(gs)
	grpct.RegisterKV(gs, srv)
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Fatal(err)
	}
	go func() {
		if err := gs.Serve(lis); err != nil {
			logger.Fatal(err)
		}
	}()
	logger.Printf("serving on %s (peers %v, data %s, read-index=%v batching=%v max-inflight=%d group-commit=%v)",
		*listen, peers, *dataDir, *readIndex, *batching, *inflight, *groupCommit)

	if *statusPort > 0 {
		go serveStatus(*statusPort, rf, tr, logger)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Printf("shutting down")
	gs.Stop()
	srv.Kill()
	rf.Stop()
	store.Sync() // flush anything buffered; a clean stop loses nothing
	tr.Close()
	store.Close()
}

type statusJSON struct {
	raft.Status
	Isolated bool          `json:"isolated"`
	Blocked  []raft.NodeID `json:"blocked"`
	Time     int64         `json:"timeUnixMs"`
}

func serveStatus(port int, rf *raft.Node, tr *grpct.Transport, logger *log.Logger) {
	mux := http.NewServeMux()
	cors := func(w http.ResponseWriter) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
	}
	status := func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		json.NewEncoder(w).Encode(statusJSON{Status: rf.Status(), Isolated: tr.Isolated(),
			Blocked: tr.Blocked(), Time: time.Now().UnixMilli()})
	}
	mux.HandleFunc("/status", status)
	// /admin/isolate?on=true|false : drop all Raft traffic (simulated crash
	// as far as the other nodes can tell; clients can still reach it).
	mux.HandleFunc("/admin/isolate", func(w http.ResponseWriter, r *http.Request) {
		on, _ := strconv.ParseBool(r.URL.Query().Get("on"))
		tr.Isolate(on)
		logger.Printf("admin: isolate=%v", on)
		status(w, r)
	})
	// /admin/block?peers=2,3 : drop traffic to/from those peers ("" = none).
	mux.HandleFunc("/admin/block", func(w http.ResponseWriter, r *http.Request) {
		var ids []raft.NodeID
		for _, p := range strings.Split(r.URL.Query().Get("peers"), ",") {
			if n, err := strconv.ParseUint(strings.TrimSpace(p), 10, 64); err == nil {
				ids = append(ids, raft.NodeID(n))
			}
		}
		tr.Block(ids...)
		logger.Printf("admin: blocked=%v", ids)
		status(w, r)
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	logger.Printf("status on http://localhost:%d/status", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Printf("status server: %v", err)
	}
}
