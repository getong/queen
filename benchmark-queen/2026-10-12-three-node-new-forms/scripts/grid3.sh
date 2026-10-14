#!/usr/bin/env bash
# grid3.sh <partitions>... : the partitions grid measured as the Kafka and Pulsar points of 2026-09-30 were (grid2.sh),
# for a broker binary of the caller's choice: a fresh cluster for EVERY point, 100 messages a request, deduplication
# off, completed messages kept 1,800 s and the rows window at the code default (900 s), so nothing is deleted or
# trimmed while a point runs; one queue, prefilled; 90 s a point. Each row of ../runs/$OUTF carries the figure of the
# Kafka and Pulsar report (seconds 30-60 of the load: mid_*), the last 30 s, and the brokers' cores, memory, disk and
# network. A column stops at the first load that is not carried.
# QBIN (required: the broker binary), EXTRA_ENV (broker settings), RATES, OUTF (grid-final.jsonl), PFX (fin), DUR (90).
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
RATES=${RATES:-100000 500000 1000000 1500000 2000000}
OUTF=${OUTF:-grid-final.jsonl}; PFX=${PFX:-fin}
for P in "$@"; do
  case $P in
    10)  C=1;  W=1;  B=20000;;
    100) C=11; W=1;  B=5000;;
    *)   C=33; W=10; B=1000;;
  esac
  D=""; [ "$P" -ge 10000000 ] && D=${PREDRAIN10M:-240}   # ten million: drain the prefill before the step (mrun.sh PREDRAIN)
  for R in $RATES; do
    TEN=0 PARTS=$P CONS=$C POPW=$W POPB=$B QBIN=${QBIN:?the broker binary} STEP_PY=gstep.py PREDRAIN=$D DEDUP=0 CRET=1800 TXNW= GRID_OUT=$OUTF \
      QEXTRA="-push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s" \
      "$HERE/mrun.sh" $PFX-p$P-r$R 1 sync 5 "${DUR:-90}" $R
    grep -q "\"carried\": true.*\"dir\": \"$PFX-p$P-r$R/$R\"" "$HERE/../runs/$OUTF" 2>/dev/null || { echo "[$(date -u +%H:%M:%S)] $P partitions: not carried at $R, the column stops here"; break; }
  done
done
echo GRID3_DONE
