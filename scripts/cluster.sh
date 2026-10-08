#!/usr/bin/env bash
# Run a 5-node RaftKV cluster as local processes (no Docker needed).
#
#   scripts/cluster.sh start [extra raftkv-server flags]   # wipe data, start 5 nodes
#   scripts/cluster.sh stop                                # SIGTERM all nodes
#   scripts/cluster.sh kill N                              # SIGKILL node N (a crash)
#   scripts/cluster.sh restart N                           # start node N again, same data
#   scripts/cluster.sh status                              # role/term/commit of each node
#
# Node N listens on 127.0.0.1:700N (gRPC) and :800N (/status, /admin).
set -euo pipefail
cd "$(dirname "$0")/.."
N=${NODES:-5}
DATA=${DATA:-data}
BIN=${BIN:-bin/raftkv-server}
PEERS=$(for i in $(seq 1 "$N"); do printf "%s=127.0.0.1:700%s," "$i" "$i"; done | sed 's/,$//')
FLAGS_FILE="$DATA/flags"

start_node() {
  local i=$1; shift
  mkdir -p "$DATA/n$i"
  nohup "$BIN" --id "$i" --peers "$PEERS" --data-dir "$DATA/n$i" --status-port "800$i" "$@" \
    >"$DATA/n$i.log" 2>&1 &
  echo $! >"$DATA/n$i.pid"
}

case "${1:-}" in
  start)
    shift
    [ -x "$BIN" ] || go build -o bin/ ./cmd/...
    "$0" stop >/dev/null 2>&1 || true
    rm -rf "$DATA"; mkdir -p "$DATA"
    echo "$@" >"$FLAGS_FILE"
    for i in $(seq 1 "$N"); do start_node "$i" "$@"; done
    echo "started $N nodes: $PEERS"
    ;;
  stop)
    for f in "$DATA"/n*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
    ;;
  kill)
    i=$2; kill -9 "$(cat "$DATA/n$i.pid")"; rm -f "$DATA/n$i.pid"; echo "killed n$i"
    ;;
  restart)
    i=$2
    # shellcheck disable=SC2046
    start_node "$i" $(cat "$FLAGS_FILE"); echo "restarted n$i"
    ;;
  status)
    for i in $(seq 1 "$N"); do
      s=$(curl -s --max-time 1 "localhost:800$i/status" || true)
      if [ -z "$s" ]; then echo "n$i: down"; else
        echo "n$i: $(echo "$s" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(f"{d["role"]:9} term={d["term"]} commit={d["commitIndex"]} applied={d["lastApplied"]} snap={d["snapshotIndex"]} log={d["logLength"]}")')"
      fi
    done
    ;;
  *)
    sed -n '2,9p' "$0"; exit 2 ;;
esac
