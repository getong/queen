#!/usr/bin/env bash
# sp.sh <tag> <partitions> <msg/s> : one point of the clean matrix (grid3.sh's settings) with the loaders reporting
# every second and the leader profiled in two short windows (rehashprobe.sh; W1, W2, LEN choose them). For the one
# high window of the 2M msg/s points with a million partitions or more (2026-10-13). QBIN: the broker binary.
# EXTRA_ENV: broker settings. The row goes to ../runs/grid-probe.jsonl, never to the matrix's files.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
TAG=$1; P=$2; R=$3
case $P in 10) C=1; W=1; B=20000;; 100) C=11; W=1; B=5000;; *) C=33; W=10; B=1000;; esac
D=""; [ "$P" -ge 10000000 ] && D=240
PE=""; [ -n "${W1:-}" ] && PE="$PE W1=$W1"; [ -n "${W2:-}" ] && PE="$PE W2=$W2"; [ -n "${LEN:-}" ] && PE="$PE LEN=$LEN"
TEN=0 PARTS=$P CONS=$C POPW=$W POPB=$B QBIN=${QBIN:?the broker binary} STEP_PY=gstep.py PREDRAIN=$D DEDUP=0 CRET=1800 TXNW= GRID_OUT=grid-probe.jsonl \
  PROBE=${PROBE-1} PROBE_SH=rehashprobe.sh PROBE_ENV="$PE" \
  QEXTRA="-push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s -report ${REPORT:-1}" \
  "$HERE/mrun.sh" "$TAG" 1 sync 5 "${DUR:-70}" "$R" > "$HERE/../runs/$TAG.out" 2>&1
python3 "$HERE/secline.py" "$HERE/../runs/$TAG/$R"
