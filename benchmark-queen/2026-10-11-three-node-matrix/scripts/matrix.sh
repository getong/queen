#!/usr/bin/env bash
# matrix.sh : the anchor run in the 10-10 layout, the six settings at 1M msg/s, then a ramp for each setting.
S=$(cd "$(dirname "$0")" && pwd)
TEN=0 "$S/mrun.sh" ref-1010-layout 1 sync 5 180 1000000
for G in 1 3; do
  "$S/mrun.sh" m1-g$G-sync5   $G sync   5 180 1000000
  "$S/mrun.sh" m1-g$G-sync1   $G sync   1 180 1000000
  "$S/mrun.sh" m1-g$G-nosync1 $G nosync 1 180 1000000
done
echo "PART 1 DONE $(date -u +%T)"
for C in "sync 5 sync5" "sync 1 sync1" "nosync 1 nosync1"; do set -- $C
  "$S/mrun.sh" c-g1-$3 1 $1 $2 75 1200000 1400000 1600000 1800000
done
echo "PART 2 DONE $(date -u +%T)"
for C in "sync 5 sync5" "sync 1 sync1" "nosync 1 nosync1"; do set -- $C
  "$S/mrun.sh" c-g3-$3 3 $1 $2 75 1400000 1800000 2200000 2500000
done
echo "PART 3 DONE $(date -u +%T)"
