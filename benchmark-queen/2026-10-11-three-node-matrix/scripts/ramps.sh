#!/usr/bin/env bash
# ramps.sh : the ceiling of each of the six settings. A warm-up, then steps of 75 s; a ramp stops at the first step not carried.
S=$(cd "$(dirname "$0")" && pwd)
for C in "sync 5 sync5" "sync 1 sync1" "nosync 1 nosync1"; do set -- $C
  "$S/mramp.sh" c-g1-$3 1 $1 $2 75 1200000 1300000 1400000 1500000
done
echo "RAMPS 1 DONE $(date -u +%T)"
for C in "sync 5 sync5" "sync 1 sync1" "nosync 1 nosync1"; do set -- $C
  "$S/mramp.sh" c-g3-$3 3 $1 $2 75 1500000 1800000 2100000 2400000
done
echo "RAMPS 3 DONE $(date -u +%T)"
