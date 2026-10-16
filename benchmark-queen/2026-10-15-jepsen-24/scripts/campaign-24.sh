#!/usr/bin/env bash
# 2.4 (ed73dc1cb, the three bytes-per-message fixes) on n1..n5, the control on this machine (16 GB, heap -Xmx12g).
cd /root/jepsen-24
export RUNS=/root/runs-24 BIN=/root/bin/queen-24 NODES=n1,n2,n3,n4,n5
mkdir -p $RUNS; md5sum $BIN > $RUNS/binary.md5
./run-campaign.sh "${1:-matrix-24.txt}"
echo "$(date -u +%FT%TZ) ALL DONE ${1:-matrix-24.txt}" >> $RUNS/summary.txt
