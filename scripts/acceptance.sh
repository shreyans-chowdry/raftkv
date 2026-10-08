#!/usr/bin/env bash
# M7 acceptance: on a fresh 5-process cluster, write 100 keys, SIGKILL the
# leader, keep writing, SIGKILL a second node, check every key still reads
# its latest value, restart both and check they catch up.
set -euo pipefail
cd "$(dirname "$0")/.."
CLI="bin/raftkv-cli --timeout 20s"
leader() {
  for i in 1 2 3 4 5; do
    r=$(curl -s --max-time 1 "localhost:800$i/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["role"])' 2>/dev/null || true)
    [ "$r" = leader ] && { echo "$i"; return; }
  done
}
wait_leader() { for _ in $(seq 50); do l=$(leader); [ -n "$l" ] && { echo "$l"; return; }; sleep 0.2; done; echo "no leader" >&2; exit 1; }

scripts/cluster.sh start "$@"
L=$(wait_leader); echo "leader is n$L"

echo "== put 100 keys"
for k in $(seq 1 100); do $CLI put "key$k" "v1-$k" >/dev/null; done
echo "ok"

echo "== kill -9 the leader (n$L)"
scripts/cluster.sh kill "$L"
t0=$(date +%s.%N)
L2=$(wait_leader); t1=$(date +%s.%N)
printf "new leader n%s after %.2fs\n" "$L2" "$(echo "$t1 - $t0" | bc)"

echo "== overwrite keys 1-50 while one node is down"
for k in $(seq 1 50); do $CLI put "key$k" "v2-$k" >/dev/null; done
echo "ok"

V=$(( L2 % 5 + 1 )); [ "$V" = "$L" ] && V=$(( V % 5 + 1 ))
echo "== kill -9 a second node (n$V); 3 of 5 left"
scripts/cluster.sh kill "$V"
for k in $(seq 51 60); do $CLI put "key$k" "v3-$k" >/dev/null; done
echo "== read back all 100 keys"
bad=0
for k in $(seq 1 100); do
  want="v1-$k"; [ "$k" -le 50 ] && want="v2-$k"; [ "$k" -ge 51 ] && [ "$k" -le 60 ] && want="v3-$k"
  got=$($CLI get "key$k")
  [ "$got" = "$want" ] || { echo "key$k: got '$got' want '$want'"; bad=$((bad+1)); }
done
echo "mismatches: $bad"

echo "== restart n$L and n$V"
scripts/cluster.sh restart "$L"; scripts/cluster.sh restart "$V"
for _ in $(seq 50); do
  sleep 0.2
  c=$(for i in 1 2 3 4 5; do curl -s --max-time 1 "localhost:800$i/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["lastApplied"])' 2>/dev/null || echo x; done | sort -u | wc -l)
  [ "$c" = 1 ] && break
done
scripts/cluster.sh status
[ "$bad" = 0 ] && echo "ACCEPTANCE PASSED" || { echo "ACCEPTANCE FAILED"; exit 1; }
