#!/usr/bin/env bash
# real.sh <partitions>... : the "more real" points of 2026-10-12: deduplication on (60 s window), completed messages
# deleted after 60 s, the rows window at 60 s, and the figure taken after five minutes of load: a fresh cluster for
# every point, one queue prefilled, 360 s at RATE msg/s (100 messages a request), judged on its last 60 s. By then
# every window is full and the broker deletes at the rate it is written to. Rows go to ../runs/grid-real.jsonl.
# RATE (1500000), QBIN (the broker binary), EXTRA_ENV (broker settings), DUR (360), PFX (the runs' prefix, rl),
# TAIL (the part judged, 60s).
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
RATE=${RATE:-1500000}
for P in "$@"; do
  case $P in
    10)  C=1;  W=1;  B=20000;;
    100) C=11; W=1;  B=5000;;
    *)   C=33; W=10; B=1000;;
  esac
  D=""; [ "$P" -ge 10000000 ] && D=240
  TEN=0 PARTS=$P CONS=$C POPW=$W POPB=$B QBIN=${QBIN:?the broker binary} STEP_PY=gstep.py PREDRAIN=$D DEDUP=60 CRET=60 TXNW=60 GRID_OUT=${OUTF:-grid-real.jsonl} \
    QEXTRA="-push-wire bin -push-zstd -ack-counts -pop-frames -tail ${TAIL:-60s}" \
    "$HERE/mrun.sh" ${PFX:-rl}-p$P-r$RATE 1 sync 5 "${DUR:-360}" $RATE
done
echo REAL_DONE
