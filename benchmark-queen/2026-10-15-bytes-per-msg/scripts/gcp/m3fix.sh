#!/bin/bash
# The 3M msg/s partitions matrix of 2026-10-14 (m3: grid3.sh, 70 s, a fresh cluster a point, dedup 0, completed
# retention 1,800 s, the new wire forms) with the fixed broker: 10, 100, 1k, 10k, 100k, 1M, then 10M with
# QUEEN_CONSUME_IDLE_UNLOAD_S=30 and grid3.sh's 240 s predrain, as m3's 10M point. Waits for the ceiling ladder
# (GRID3_DONE in grid-fix.out), trims the brokers' disks, runs. Lines runs/matrix-fix-3m.out, rows grid-fix-m3.jsonl.
G=$(cd "$(dirname "$0")" && pwd)
W=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms" && pwd)
OUT=$W/runs/matrix-fix-3m.out
echo "[$(date -u +%H:%M:%S)] queued: waiting for the ceiling ladder to end" >> $OUT
until grep -q GRID3_DONE $W/runs/grid-fix.out 2>/dev/null; do sleep 5; done
echo "[$(date -u +%H:%M:%S)] ladder done, trimming" >> $OUT
bash $G/fstrim.sh >> $OUT 2>&1
echo "[$(date -u +%H:%M:%S)] matrix at 3M, fixed broker: 10 100 1000 10000 100000 1000000" >> $OUT
QBIN=fix DUR=70 PFX=fm3 OUTF=grid-fix-m3.jsonl RATES=3000000 \
  bash $W/scripts/grid3.sh 10 100 1000 10000 100000 1000000 >> $OUT 2>&1
echo "[$(date -u +%H:%M:%S)] 10M with QUEEN_CONSUME_IDLE_UNLOAD_S=30, trimming first" >> $OUT
bash $G/fstrim.sh >> $OUT 2>&1
EXTRA_ENV=QUEEN_CONSUME_IDLE_UNLOAD_S=30 QBIN=fix DUR=70 PFX=fm3 OUTF=grid-fix-m3.jsonl RATES=3000000 \
  bash $W/scripts/grid3.sh 10000000 >> $OUT 2>&1
echo M3FIX_DONE >> $OUT
