#!/bin/bash
# One line per 1 KB matrix point as it lands: its partition count and gstep verdict; failures and the end marker.
OUT=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms/runs" && pwd)/matrix-fix-1m1k.out
until [ -f $OUT ]; do sleep 1; done
tail -n +1 -f $OUT | awk '
  /run f1k-p[0-9]+-r/ { match($0, /f1k-p[0-9]+/); p = substr($0, RSTART + 5, RLENGTH - 5) }
  /1,000,000: |FAILED|not carried at|did not become healthy|M1KFIX_DONE|repeats done|RUNNING, no trim/ { print (p != "" ? p " partitions: " : "") $0; fflush() }
  /M1KFIX_DONE/ { exit }'
