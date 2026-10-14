#!/usr/bin/env bash
# 2.3.0-beta.2 on n1..n5, the control on this machine (2 vCPU, 4 GB: the JVM heap is capped at 2.6 GB in project.clj).
cd /root/jepsen-230
export RUNS=/root/runs-230 BIN=/root/bin/queen-230b2 NODES=n1,n2,n3,n4,n5
./run-campaign.sh "${1:-matrix-230.txt}"
echo "$(date -u +%FT%TZ) ALL DONE ${1:-matrix-230.txt}" >> $RUNS/summary.txt
