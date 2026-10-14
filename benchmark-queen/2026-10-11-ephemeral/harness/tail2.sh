#!/usr/bin/env bash
S=$(cd "$(dirname "$0")" && pwd); CK1="QUEEN_CONSUME_CHECKPOINT_MS=1"
DUR=30 MODE=nosync EXTRA="$CK1" DBB=0 "$S/grun5.sh" t2-g3-nosync1-vmdefault 3 1000000
DUR=30 MODE=nosync EXTRA="$CK1" DBB=67108864 "$S/grun5.sh" t2-g3-nosync1-bg64m 3 1000000
DUR=20 MODE=sync EXTRA="QUEEN_RAFT_DURABLE_EVERY_MS=5000" DBB=0 "$S/grun5.sh" t2-g3-sync5-dur5000 3 1000000
echo "TAIL2 DONE $(date -u +%T)"
