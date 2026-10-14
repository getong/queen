#!/usr/bin/env bash
# watch-expiry.sh <out.tsv> <seconds> [port]  (on the VM)
# Every 5 s: the expiry backlog of a node, the rows its sweep deleted, its RSS
# and the size of its data directory.
set -u
OUT=${1:?out}
SECS=${2:?seconds}
PORT=${3:-6632}
PID=$(pgrep -x queen | head -1)
DIR=$(tr '\0' '\n' </proc/$PID/environ | sed -n 's/^QUEEN_RAFT_DIR=//p')
printf "t\texpired_not_pruned\tcapped\texpiry_lag_s\trows_deleted_total\trss_mib\tdata_mib\n" >"$OUT"
T0=$(date +%s)
while :; do
  NOW=$(($(date +%s) - T0))
  M=$(curl -s -m 3 "http://127.0.0.1:$PORT/metrics/prometheus")
  val() { echo "$M" | awk -v n="$1" '$1==n {print $2}' | head -1; }
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$NOW" "$(val queen_kv_expired_not_pruned)" \
    "$(val queen_kv_expired_not_pruned_capped)" "$(val queen_kv_expiry_lag_seconds)" \
    "$(val 'queen_raft_store_operations_total{kind="rows_deleted"}')" \
    "$(awk '/VmRSS/ {printf "%d", $2/1024}' /proc/$PID/status)" \
    "$(du -sm "$DIR" | cut -f1)" >>"$OUT"
  [ "$NOW" -ge "$SECS" ] && break
  sleep 5
done
