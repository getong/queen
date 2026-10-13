#!/usr/bin/env bash
# tac.sh <tag> <partitions> <msg/s> : one tactical point on a fresh cluster, then its line (tacline.py). The settings
# are the rerun's (grid2.sh: dedup off, completed messages kept 1,800 s, the rows window at its default) unless the
# caller's environment says otherwise: QBIN (the broker binary), EXTRA_ENV (broker settings), DEDUP, CRET, TXNW, DUR
# (seconds, 90), BATCH (messages a request, 100), CONS / POPW / POPB (the consumers), PREDRAIN, PROBE, QEXTRA.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
TAG=$1; P=$2; R=$3
case $P in 10) C=1; W=1; B=20000;; 100) C=11; W=1; B=5000;; *) C=33; W=10; B=1000;; esac
D=${PREDRAIN-}; [ -z "$D" ] && [ "$P" -ge 10000000 ] && D=240
TEN=0 PARTS=$P CONS=${CONS:-$C} POPW=${POPW:-$W} POPB=${POPB:-$B} QBIN=${QBIN:-p1} STEP_PY=gstep.py PREDRAIN=$D \
  DEDUP=${DEDUP:-0} CRET=${CRET:-1800} TXNW=${TXNW-} GRID_OUT=tactical.jsonl BATCH=${BATCH:-100} \
  QEXTRA="${QEXTRA:--push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s}" \
  "$HERE/mrun.sh" "$TAG" 1 sync 5 "${DUR:-90}" "$R" > "$HERE/../runs/$TAG.out" 2>&1
python3 "$HERE/tacline.py" "$HERE/../runs/$TAG/$R"
