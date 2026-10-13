#!/usr/bin/env bash
# grid.sh <partitions>... : the partitions grid of 2026-10-12. For every partition count: one fresh cluster (binary 23,
# its defaults), one queue with that many partitions, prefilled (created and written to once), then 90 s at 100k,
# 500k, 1M, 1.5M and 2M msg/s, judged on the last 30 s (gstep.py); it stops at the first load not carried.
# The new wire forms are on. With 10 and 100 partitions the consumers take one partition each with a large pop batch
# (a partition is held by one consumer at a time): 1 a process x 20,000 and 11 a process x 5,000.
# RATES overrides the loads.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
RATES=${RATES:-100000 500000 1000000 1500000 2000000}
for P in "$@"; do
  case $P in
    10)  C=1;  W=1;  B=20000;;
    100) C=11; W=1;  B=5000;;
    *)   C=33; W=10; B=1000;;
  esac
  D=""; [ "$P" -ge 10000000 ] && D=240   # ten million: drain the prefill before the first step (mrun.sh PREDRAIN)
  TEN=0 PARTS=$P CONS=$C POPW=$W POPB=$B QBIN=23 STEP_PY=gstep.py PREDRAIN=$D \
    QEXTRA="-push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s" \
    "$HERE/mrun.sh" grid-p$P 1 sync 5 90 $RATES
done
echo GRID_DONE >> "$HERE/../runs/grid.log"
