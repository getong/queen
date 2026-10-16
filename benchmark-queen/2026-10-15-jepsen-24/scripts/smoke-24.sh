#!/usr/bin/env bash
# The setup check (as for 2.3.0: s-w8-none), with the cluster version of every node sampled while it runs.
cd /root/jepsen-24
printf 's-w8-none     --workload retention --nemesis none --time-limit 60\n' > matrix-smoke-24.txt
export RUNS=/root/runs-24-smoke BIN=/root/bin/queen-24 NODES=n1,n2,n3,n4,n5
mkdir -p $RUNS; md5sum $BIN > $RUNS/binary.md5
( while :; do for n in n1 n2 n3 n4 n5; do echo "$(date -u +%T) $n $(python3 /root/roll/health.py $n)"; done; sleep 5; done ) > $RUNS/cv.txt 2>&1 &
W=$!
./run-campaign.sh matrix-smoke-24.txt
kill $W
echo "$(date -u +%FT%TZ) ALL DONE matrix-smoke-24.txt" >> $RUNS/summary.txt
