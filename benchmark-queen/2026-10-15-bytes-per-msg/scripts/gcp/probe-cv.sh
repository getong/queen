#!/bin/bash
# Every 20 s while the A/B runs: each broker's committed cluster version and the version it reads (/health raft
# clusterVersion, kinds), so a fix run is known to have written the version-7 forms. Stops at AB_GCP_DONE.
. "$(dirname "$0")/hosts.sh"
OUT=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms/runs" && pwd)/bpm-ab.out
while ! grep -q AB_GCP_DONE $OUT 2>/dev/null; do
  line="[$(date -u +%H:%M:%S)]"
  for i in 0 1 2; do
    v=$($SSH root@${BP[$i]} "curl -s -m 2 http://${BV[$i]}:6632/health" 2>/dev/null | grep -o '"clusterVersion":[0-9]*,"kinds":[0-9]*' | tr -d '"' | sed 's/clusterVersion:/cv /; s/,kinds:/ kinds /')
    line="$line n$((i+1)) ${v:-down}"
  done
  echo "$line"
  sleep 20
done
