// Command raftkv-dash serves the live cluster view: a page that polls every
// node's /status and draws roles, terms and log indexes, with buttons that
// isolate nodes or partition the cluster through the nodes' /admin
// endpoints.
//
//	raftkv-dash --nodes 1=localhost:8001,2=localhost:8002,... --listen :9000
//
// The dashboard proxies all requests to the nodes, so the page itself only
// talks to the dashboard (no CORS, no node addresses in the browser).
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/shreyans-chowdry/raftkv/raft"
	grpct "github.com/shreyans-chowdry/raftkv/transport/grpc"
	"github.com/shreyans-chowdry/raftkv/web"
)

func main() {
	nodesFlag := flag.String("nodes", "1=localhost:8001,2=localhost:8002,3=localhost:8003,4=localhost:8004,5=localhost:8005",
		"status endpoints: id=host:port,...")
	listen := flag.String("listen", ":9000", "address to serve the dashboard on")
	flag.Parse()
	nodes, err := grpct.ParsePeers(*nodesFlag)
	if err != nil {
		log.Fatal(err)
	}
	var ids []raft.NodeID
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	client := &http.Client{Timeout: 400 * time.Millisecond}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(web.Files)))

	// GET /api/status -> {"nodes":[{"id":1,"up":true,"status":{...}}, ...]}
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		type nodeStatus struct {
			ID     raft.NodeID     `json:"id"`
			Up     bool            `json:"up"`
			Status json.RawMessage `json:"status,omitempty"`
		}
		out := make([]nodeStatus, len(ids))
		var wg sync.WaitGroup
		for i, id := range ids {
			wg.Add(1)
			go func(i int, id raft.NodeID) {
				defer wg.Done()
				out[i] = nodeStatus{ID: id}
				resp, err := client.Get("http://" + nodes[id] + "/status")
				if err != nil {
					return
				}
				defer resp.Body.Close()
				b, err := io.ReadAll(resp.Body)
				if err == nil && resp.StatusCode == 200 {
					out[i].Up, out[i].Status = true, b
				}
			}(i, id)
		}
		wg.Wait()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"nodes": out, "time": time.Now().UnixMilli()})
	})

	// POST /api/admin?node=N&action=isolate|rejoin|block&peers=1,2
	mux.HandleFunc("/api/admin", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		n, err := strconv.ParseUint(q.Get("node"), 10, 64)
		addr, ok := nodes[raft.NodeID(n)]
		if err != nil || !ok {
			http.Error(w, "unknown node", http.StatusBadRequest)
			return
		}
		var target string
		switch q.Get("action") {
		case "isolate":
			target = "/admin/isolate?on=true"
		case "rejoin":
			target = "/admin/isolate?on=false"
		case "block":
			target = "/admin/block?peers=" + url.QueryEscape(q.Get("peers"))
		default:
			http.Error(w, "unknown action", http.StatusBadRequest)
			return
		}
		resp, err := client.Get("http://" + addr + target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		io.Copy(w, resp.Body)
	})

	log.Printf("dashboard on http://localhost%s (nodes %v)", *listen, nodes)
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
