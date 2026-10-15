#!/bin/bash
# Two more runs of the matrix's 1,000-partition point at 3M msg/s (her "run the 1,000-partition point at 3M twice
# more, fresh cluster each, same settings"): grid3.sh 1000 as m3fix.sh ran it, PFX fm3r1 and fm3r2, after a trim.
# Lines runs/rep1k.out, rows grid-fix-rep.jsonl.
G=$(cd "$(dirname "$0")" && pwd)
W=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms" && pwd)
OUT=$W/runs/rep1k.out
echo "[$(date -u +%H:%M:%S)] trimming" >> $OUT
bash $G/fstrim.sh >> $OUT 2>&1
for r in 1 2; do
  QBIN=fix DUR=70 PFX=fm3r$r OUTF=grid-fix-rep.jsonl RATES=3000000 bash $W/scripts/grid3.sh 1000 >> $OUT 2>&1
done
echo REP1K_DONE >> $OUT
