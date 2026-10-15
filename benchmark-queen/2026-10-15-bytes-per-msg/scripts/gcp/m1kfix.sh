#!/bin/bash
# The 1 KB matrix of 2026-10-14 (p1k: 1,000,000 msg/s of 1,024-byte payloads, grid3.sh, 70 s, a fresh cluster a
# point, the m3 settings otherwise) with the fixed broker: 10, 100, 1k, 10k, 100k, 1M, then 10M with
# QUEEN_CONSUME_IDLE_UNLOAD_S=30 and grid3.sh's 240 s predrain, as p1k's 10M point. Waits for the 1k repeats
# (REP1K_DONE in rep1k.out), trims the brokers' disks, runs. Lines runs/matrix-fix-1m1k.out, rows grid-fix-1m1k.jsonl.
G=$(cd "$(dirname "$0")" && pwd)
W=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms" && pwd)
OUT=$W/runs/matrix-fix-1m1k.out
echo "[$(date -u +%H:%M:%S)] queued: waiting for the 1k repeats to end" >> $OUT
until grep -q REP1K_DONE $W/runs/rep1k.out 2>/dev/null; do sleep 5; done
echo "[$(date -u +%H:%M:%S)] repeats done, trimming" >> $OUT
bash $G/fstrim.sh >> $OUT 2>&1
echo "[$(date -u +%H:%M:%S)] matrix at 1M x 1 KB, fixed broker: 10 100 1000 10000 100000 1000000" >> $OUT
PAYLOAD=1024 QBIN=fix DUR=70 PFX=f1k OUTF=grid-fix-1m1k.jsonl RATES=1000000 \
  bash $W/scripts/grid3.sh 10 100 1000 10000 100000 1000000 >> $OUT 2>&1
echo "[$(date -u +%H:%M:%S)] 10M with QUEEN_CONSUME_IDLE_UNLOAD_S=30, trimming first" >> $OUT
bash $G/fstrim.sh >> $OUT 2>&1
EXTRA_ENV=QUEEN_CONSUME_IDLE_UNLOAD_S=30 PAYLOAD=1024 QBIN=fix DUR=70 PFX=f1k OUTF=grid-fix-1m1k.jsonl RATES=1000000 \
  bash $W/scripts/grid3.sh 10000000 >> $OUT 2>&1
echo M1KFIX_DONE >> $OUT
