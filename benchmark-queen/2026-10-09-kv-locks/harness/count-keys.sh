#!/usr/bin/env bash
# count-keys.sh <out.txt> <samples> <seconds between> [port]  (on the VM)
# The KV rows a node holds, live and expired together, summed over namespaces.
set -u
OUT=${1:?out}
N=${2:?samples}
GAP=${3:?seconds}
PORT=${4:-6632}
: >"$OUT"
for i in $(seq 1 "$N"); do
  echo "$(date -u +%H:%M:%S) $(date +%s.%N) $(curl -s -m 30 "http://127.0.0.1:$PORT/api/v1/resources/kv/namespaces" |
    jq '[.namespaces[].keys] | add')" >>"$OUT"
  [ "$i" -lt "$N" ] && sleep "$GAP"
done
