#!/usr/bin/env bash
# cons.sh <tag> <partitions> <msg/s> <consumers a process> <pop width> <pop batch> : one point of the partitions grid
# (grid.sh) alone, on a fresh cluster, with a consumer setup of one's choice: is a load that the grid did not carry a
# limit of the broker or of how the consumers were set up?
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
TEN=0 PARTS=$2 CONS=$4 POPW=$5 POPB=$6 QBIN=23 STEP_PY=gstep.py \
  QEXTRA="-push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s" \
  "$HERE/mrun.sh" "$1" 1 sync 5 90 "$3"
