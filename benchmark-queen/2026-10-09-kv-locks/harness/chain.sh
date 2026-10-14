#!/usr/bin/env bash
# chain.sh <after-marker> <phase>...: wait for a phase to end, then run the next ones in order.
cd /root/kvl
while [ ! -e "runs/DONE-$1" ]; do sleep 2; done
sleep 2
pkill -f "matrix.sh $1" 2>/dev/null
pkill -x kvlockload 2>/dev/null
shift
for p in "$@"; do ./matrix.sh "$p"; done
date -u +%FT%TZ > runs/CHAIN-DONE
