#!/usr/bin/env bash
# fast.sh : the remaining ceilings in short steps (40 s). Three groups have no start-up dip, so they get no warm-up.
S=$(cd "$(dirname "$0")" && pwd)
"$S/mrun.sh" c-g3-sync5   3 sync   5 40 1800000 2100000 2400000
"$S/mrun.sh" c-g3-nosync1 3 nosync 1 40 1800000 2100000 2400000
WARM=70 "$S/mramp.sh" c-g1-nosync1 1 nosync 1 40 1300000
"$S/mrun.sh" c-g3-sync1   3 sync   1 40 1800000 2100000
echo "FAST DONE $(date -u +%T)"
