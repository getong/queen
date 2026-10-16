#!/bin/bash
# The fixed broker's ceiling on 1,000 partitions, as 2026-10-14's stock one (grid3.sh 1000, fresh cluster a point,
# 70 s, dedup 0, completed retention 1,800 s, the new wire forms, judged on the last 30 s by gstep.py), from 3.5M in
# 0.5M steps up to 8M; grid3.sh stops at the first rate not carried. Lines in runs/grid-fix.out, rows grid-fix.jsonl.
W=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms" && pwd)
OUT=$W/runs/grid-fix.out
echo "[$(date -u +%H:%M:%S)] fix ceiling at 1,000 partitions, 3.5M up in 0.5M steps" >> $OUT
QBIN=fix DUR=70 PFX=fxc OUTF=grid-fix.jsonl \
  RATES="3500000 4000000 4500000 5000000 5500000 6000000 6500000 7000000 7500000 8000000" \
  bash $W/scripts/grid3.sh 1000 >> $OUT 2>&1
