#!/usr/bin/env bash
# single.sh <partitions> <req/s>... : requests of ONE message, to find how many a second the cluster sustains at a
# partition count. One fresh cluster, one queue prefilled, then a 90 s step per rate, ascending, judged on its last
# 30 s; it stops at the first rate that is not carried. Deduplication on (60 s), completed messages deleted after
# 60 s, rows window 60 s: what a queue is run with, so that a rate carried here is one that can be kept up.
# Rows go to ../runs/grid-single.jsonl. QBIN (the broker binary), EXTRA_ENV, QEXTRA (qload's wire flags), DUR (90).
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
P=$1; shift
case $P in
  10)  C=1;  W=1;  B=20000;;
  100) C=11; W=1;  B=5000;;
  *)   C=33; W=10; B=1000;;
esac
D=""; [ "$P" -ge 10000000 ] && D=240
TEN=0 PARTS=$P CONS=${CONS:-$C} POPW=${POPW:-$W} POPB=${POPB:-$B} QBIN=${QBIN:?the broker binary} STEP_PY=gstep.py PREDRAIN=$D \
  DEDUP=${DEDUP:-60} CRET=${CRET:-60} TXNW=${TXNW-60} GRID_OUT=${GRID_OUT:-grid-single.jsonl} BATCH=1 \
  QEXTRA="${QEXTRA:--push-wire bin -ack-counts -pop-frames -tail 30s}" \
  "$HERE/mrun.sh" "${TAG:-sg-p$P}" 1 sync 5 "${DUR:-90}" "$@"
