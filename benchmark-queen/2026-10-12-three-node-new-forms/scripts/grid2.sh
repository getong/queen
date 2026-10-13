#!/usr/bin/env bash
# grid2.sh <partitions>... : the partitions grid of grid.sh, measured as the Kafka and Pulsar points of 2026-09-30 were:
# a fresh cluster for EVERY point, deduplication off, completed messages kept 1,800 s and the rows window at the code
# default (900 s), so that nothing is deleted or trimmed while a point runs. Same loads, consumers and wire forms as
# grid.sh: 90 s a point, judged on the last 30 s. A column stops at the first load that is not carried.
# Rows go to ../runs/grid-nodel.jsonl. RATES overrides the loads.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
RATES=${RATES:-100000 500000 1000000 1500000 2000000}
OUTF=grid-nodel.jsonl
for P in "$@"; do
  case $P in
    10)  C=1;  W=1;  B=20000;;
    100) C=11; W=1;  B=5000;;
    *)   C=33; W=10; B=1000;;
  esac
  D=""; [ "$P" -ge 10000000 ] && D=240   # ten million: drain the prefill before the step (mrun.sh PREDRAIN)
  for R in $RATES; do
    TEN=0 PARTS=$P CONS=$C POPW=$W POPB=$B QBIN=23 STEP_PY=gstep.py PREDRAIN=$D DEDUP=0 CRET=1800 TXNW= GRID_OUT=$OUTF \
      QEXTRA="-push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s" \
      "$HERE/mrun.sh" nd-p$P-r$R 1 sync 5 90 $R
    grep -q "\"carried\": true.*\"dir\": \"nd-p$P-r$R/$R\"" "$HERE/../runs/$OUTF" 2>/dev/null || { echo "[$(date -u +%H:%M:%S)] $P partitions: not carried at $R, the column stops here"; break; }
  done
done
echo GRID2_DONE
