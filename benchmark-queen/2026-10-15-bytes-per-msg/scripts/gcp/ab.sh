#!/bin/bash
# Stock 2.3.0 vs the bytes-per-msg fixes at 3M msg/s on 1,000 partitions, 70 s, yesterday's cs-p1000-r3000000
# settings (EXTRA empty: TXNW set empty; dedup 0; completed retention 1800 s; checkpoint 5 ms; the new wire forms).
# Order: stock, fix, stock, fix (two of each for the noise). Lines in runs/bpm-ab.out.
W=$(cd "$(dirname "$0")/../../../2026-10-12-three-node-new-forms" && pwd)
cd $W/scripts
OUT=$W/runs/bpm-ab.out
QE='-push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s'
for round in 1 2; do
  for bin in 230 fix; do
    tag=bpm${round}-$bin-p1000
    echo "[$(date -u +%H:%M:%S)] start $tag" >> $OUT
    TEN=0 PARTS=1000 CRET=1800 DEDUP=0 TXNW= QBIN=$bin QEXTRA="$QE" bash mrun.sh $tag 1 sync 5 70 3000000 >> $OUT 2>&1
    echo "[$(date -u +%H:%M:%S)] done $tag" >> $OUT
  done
done
echo AB_GCP_DONE >> $OUT
