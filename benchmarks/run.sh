#!/usr/bin/env bash
# Full benchmark protocol for RaftKV. Writes raw CSVs to
# benchmarks/results/raw/ and the summary table to benchmarks/results/summary.md.
#
# For each variant (the optimisation ladder, one change at a time) and each
# repeat, it starts a fresh 5-process cluster on this machine and runs
# raftkv-bench at every concurrency level: closed-loop clients, 50% Put /
# 50% Get, 64-byte values, 10k keys, WARMUP s warm-up then DURATION s
# measured. The summary reports the median of the repeats. Then it runs the
# fault-tolerance test: 32 clients for 60s, kill -9 the leader at 20s and
# another node at 40s.
#
# Knobs (environment): WARMUP=10s DURATION=60s REPEATS=3 CONCURRENCY=1,8,32,64
#                      VARIANTS="baseline batching ..." (default: all)
# Plug the laptop in and close other apps first.
set -euo pipefail
cd "$(dirname "$0")/.."
WARMUP=${WARMUP:-10s}
DURATION=${DURATION:-60s}
REPEATS=${REPEATS:-3}
CONCURRENCY=${CONCURRENCY:-1,8,32,64}
OUT=benchmarks/results
RAW=$OUT/raw

# Each step adds one optimisation to the previous one. (case statements
# rather than associative arrays so this runs on macOS's bash 3.2)
ORDER="baseline batching pipelining groupcommit readindex final"
flags_for() {
  case $1 in
    baseline)    echo "--batching=false --max-inflight=1 --group-commit=false --read-index=false --parallel-leader-write=false" ;;
    batching)    echo "--max-inflight=1 --group-commit=false --read-index=false --parallel-leader-write=false" ;;
    pipelining)  echo "--group-commit=false --read-index=false --parallel-leader-write=false" ;;
    groupcommit) echo "--read-index=false --parallel-leader-write=false" ;;
    readindex)   echo "--parallel-leader-write=false" ;;
    final)       echo "" ;;
  esac
}
desc_for() {
  case $1 in
    baseline)    echo "one entry per AppendEntries, one in flight per follower, fsync per update, Gets through the log" ;;
    batching)    echo "+ batching: leader packs all pending entries into one AppendEntries" ;;
    pipelining)  echo "+ pipelining: up to 64 AppendEntries in flight per follower" ;;
    groupcommit) echo "+ group commit: one fsync per event-loop iteration" ;;
    readindex)   echo "+ ReadIndex: Gets confirmed by a heartbeat round instead of a log entry" ;;
    final)       echo "+ leader sends AppendEntries before its own fsync (all defaults)" ;;
  esac
}
now() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }
VARIANTS=${VARIANTS:-$ORDER}

go build -o bin/ ./cmd/...
mkdir -p "$RAW"

# Environment.
ENV=$OUT/environment.txt
{
  echo "date:     $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "os:       $(uname -srm)"
  if command -v lscpu >/dev/null; then
    echo "cpu:      $(lscpu | sed -n 's/^Model name:[[:space:]]*//p') ($(nproc) logical CPUs)"
    echo "memory:   $(free -g | awk '/Mem:/{print $2" GiB"}')"
  else
    echo "cpu:      $(sysctl -n machdep.cpu.brand_string 2>/dev/null) ($(sysctl -n hw.ncpu) logical CPUs)"
    echo "memory:   $(( $(sysctl -n hw.memsize) / 1073741824 )) GiB"
  fi
  echo "go:       $(go version | cut -d' ' -f3-)"
  echo "cluster:  5 raftkv-server processes + raftkv-bench on the same machine, loopback network"
  echo "storage:  $(df -h benchmarks | awk 'NR==2{print $1}') (data dir ./data)"
} >"$ENV"
cat "$ENV"

leader() {
  for i in 1 2 3 4 5; do
    r=$(curl -s --max-time 0.3 "localhost:800$i/status" 2>/dev/null | grep -o '"role":"leader"' || true)
    [ -n "$r" ] && { echo "$i"; return; }
  done
  echo ""
}
wait_leader() { for _ in $(seq 100); do l=$(leader); [ -n "$l" ] && return; sleep 0.1; done; echo "no leader" >&2; exit 1; }

for v in $VARIANTS; do
  for rep in $(seq 1 "$REPEATS"); do
    csv="$RAW/$v-r$rep.csv"
    rm -f "$csv"
    echo "== $v, repeat $rep/$REPEATS ($(flags_for "$v"))"
    # shellcheck disable=SC2046
    scripts/cluster.sh start $(flags_for "$v") >/dev/null
    wait_leader
    bin/raftkv-bench --concurrency "$CONCURRENCY" --warmup "$WARMUP" --duration "$DURATION" \
      --label "$v" --csv "$csv"
    scripts/cluster.sh stop
    sleep 1
  done
done

# Fault tolerance: 32 clients, 60s measured; kill -9 the leader at 20s and
# another node at 40s, and time how long until a new leader exists.
FT=$OUT/fault-tolerance.md
echo "== fault tolerance"
scripts/cluster.sh start >/dev/null
wait_leader
bin/raftkv-bench --concurrency 32 --warmup 5s --duration 60s --label faults \
  --csv "$RAW/faults.csv" --timeline "$OUT/fault-timeline.csv" >"$OUT/fault-bench.txt" &
BENCH=$!
sleep 25
L1=$(leader); t0=$(now); scripts/cluster.sh kill "$L1" >/dev/null
while [ -z "$(leader)" ] || [ "$(leader)" = "$L1" ]; do sleep 0.05; done
r1=$(awk -v a="$(now)" -v b="$t0" 'BEGIN{printf "%.2f", a-b}')
sleep "$(awk -v r="$r1" 'BEGIN{printf "%.2f", (20-r > 0 ? 20-r : 0)}')"
L2=$(leader); t0=$(now); scripts/cluster.sh kill "$L2" >/dev/null
while [ -z "$(leader)" ] || [ "$(leader)" = "$L2" ]; do sleep 0.05; done
r2=$(awk -v a="$(now)" -v b="$t0" 'BEGIN{printf "%.2f", a-b}')
wait $BENCH
scripts/cluster.sh stop
awk -F, -v r1="$r1" -v r2="$r2" -v l1="$L1" -v l2="$L2" '
  NR>1 { t=$1; v=$2; n++; T[n]=t; V[n]=v }
  END {
    for (i=1;i<=n;i++) { if (T[i]>=2 && T[i]<19.5) {a+=V[i]; na++} if (T[i]>=22 && T[i]<39.5) {b+=V[i]; nb++} if (T[i]>=42 && T[i]<59.5) {c+=V[i]; nc++} }
    # lowest 1-second window around each kill
    m1=1e18; m2=1e18
    for (i=10;i<=n;i++) { s=0; for (j=i-9;j<=i;j++) s+=V[j]; s/=10; if (T[i]>=19.5 && T[i]<25) { if (s<m1) m1=s } if (T[i]>=39.5 && T[i]<45) { if (s<m2) m2=s } }
    printf "32 clients, 60 s measured, 100 ms samples in `fault-timeline.csv`.\n\n"
    printf "| phase | ops/s |\n|---|---:|\n"
    printf "| 5 nodes up (2-19 s) | %.0f |\n", a/na
    printf "| lowest 1 s window after kill -9 of leader n%s at 20 s | %.0f |\n", l1, m1
    printf "| 4 nodes up (22-39 s) | %.0f |\n", b/nb
    printf "| lowest 1 s window after kill -9 of leader n%s at 40 s | %.0f |\n", l2, m2
    printf "| 3 nodes up (42-59 s) | %.0f |\n\n", c/nc
    printf "Time from kill -9 of the leader until a new leader answered /status: %.2f s (first kill), %.2f s (second kill).\n", r1, r2
  }' "$OUT/fault-timeline.csv" >"$FT"
cat "$FT"

PROTOCOL="5-node cluster on one machine (5 processes, loopback). raftkv-bench: closed-loop clients, 50% Put / 50% Get, 64-byte values, 10,000 keys, ${WARMUP} warm-up then ${DURATION} measured per concurrency level, ${REPEATS} repeats each on a fresh cluster; tables show the median of the repeats. Latency is client-observed (request sent to reply received), exact percentiles over every operation."
DESCS=""
for v in $ORDER; do DESCS="$DESCS$v=$(desc_for "$v");"; done
go run ./benchmarks/summarize --variants "$(echo "$VARIANTS" | tr ' ' ',')" --desc "$DESCS" \
  --env "$ENV" --protocol "$PROTOCOL" --faults "$FT" --out "$OUT/summary.md" >/dev/null
echo "wrote $OUT/summary.md"
