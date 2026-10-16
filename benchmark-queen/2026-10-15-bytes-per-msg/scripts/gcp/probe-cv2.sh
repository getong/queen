#!/bin/bash
# probe-cv.sh for the ceiling ladder: each broker's cluster version and kinds every 20 s, until GRID3_DONE.
. "$(dirname "$0")/hosts.sh"
OUT=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms/runs" && pwd)/grid-fix.out
while ! grep -q GRID3_DONE $OUT 2>/dev/null; do
  line="[$(date -u +%H:%M:%S)]"
  for i in 0 1 2; do
    v=$($SSH root@${BP[$i]} "curl -s -m 2 http://${BV[$i]}:6632/health" 2>/dev/null | grep -o '"clusterVersion":[0-9]*,"kinds":[0-9]*' | tr -d '"' | sed 's/clusterVersion:/cv /; s/,kinds:/ kinds /')
    line="$line n$((i+1)) ${v:-down}"
  done
  echo "$line"
  sleep 20
done
