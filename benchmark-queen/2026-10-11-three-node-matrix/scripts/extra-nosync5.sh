#!/usr/bin/env bash
# extra-nosync5.sh <log of the ramps batch> : after the ramps, the two cells the matrix lacks: fsync off with the default 5 ms checkpoint.
S=$(cd "$(dirname "$0")" && pwd)
while ! grep -q "RAMPS 3 DONE" "$1" 2>/dev/null; do sleep 10; done
"$S/mrun.sh" m1-g1-nosync5 1 nosync 5 180 1000000
"$S/mrun.sh" m1-g3-nosync5 3 nosync 5 180 1000000
echo "EXTRA DONE $(date -u +%T)"
