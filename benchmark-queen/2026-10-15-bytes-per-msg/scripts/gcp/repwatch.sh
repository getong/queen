#!/bin/bash
# One line per repeat as it lands (its tag and gstep verdict), failures, and the end marker.
OUT=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms/runs" && pwd)/rep1k.out
until [ -f $OUT ]; do sleep 1; done
tail -n +1 -f $OUT | awk '
  /run fm3r[0-9]-p1000-r/ { match($0, /fm3r[0-9]/); t = substr($0, RSTART, RLENGTH) }
  /3,000,000: |FAILED|not carried at|did not become healthy|REP1K_DONE|RUNNING, no trim/ { print (t != "" ? t ": " : "") $0; fflush() }
  /REP1K_DONE/ { exit }'
