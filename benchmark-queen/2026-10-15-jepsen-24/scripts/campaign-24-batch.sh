#!/usr/bin/env bash
# The batched-push tests (matrix-24-batch.txt, harness /root/jepsen-batch = test/jepsen with --batch-sends) on n1..n5,
# once the failover list has ended: they need the same nodes.
cd /root/jepsen-batch
until grep -q "ALL DONE matrix-220b2.txt" /root/runs-24-fo/summary.txt 2>/dev/null; do sleep 30; done
export RUNS=/root/runs-24-batch BIN=/root/bin/queen-24 NODES=n1,n2,n3,n4,n5
mkdir -p $RUNS; md5sum $BIN > $RUNS/binary.md5
./run-campaign.sh matrix-24-batch.txt
echo "$(date -u +%FT%TZ) ALL DONE matrix-24-batch.txt" >> $RUNS/summary.txt
