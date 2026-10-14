#!/usr/bin/env bash
# bracket.sh <log of fast.sh> : after it, the lower steps for three groups with the defaults (1.8M was not carried).
S=$(cd "$(dirname "$0")" && pwd)
while ! grep -q "FAST DONE" "$1" 2>/dev/null; do sleep 3; done
"$S/mrun.sh" c-g3-sync5-low 3 sync 5 40 1400000 1600000
echo "BRACKET DONE $(date -u +%T)"
