#!/bin/bash
# One line per matrix point as it lands: its partition count and gstep verdict; also failures and the end marker.
OUT=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms/runs" && pwd)/matrix-fix-3m.out
until [ -f $OUT ]; do sleep 1; done
tail -n +1 -f $OUT | awk '
  /run fm3-p[0-9]+-r/ { match($0, /fm3-p[0-9]+/); p = substr($0, RSTART + 5, RLENGTH - 5) }
  /3,000,000: |FAILED|not carried at|did not become healthy|M3FIX_DONE|ladder done/ { print (p != "" ? p " partitions: " : "") $0; fflush() }
  /M3FIX_DONE/ { exit }'
