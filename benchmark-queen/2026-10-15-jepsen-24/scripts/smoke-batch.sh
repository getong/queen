#!/usr/bin/env bash
# The batched-push modes on n6 alone (a one-voter cluster), beside the main campaign on n1..n5, at the lowest CPU priority.
cd /root/jepsen-batch
export RUNS=/root/runs-batch-smoke BIN=/root/bin/queen-24 NODES=n6
mkdir -p $RUNS
nice -n 19 ./run-campaign.sh matrix-batch-smoke.txt
echo "$(date -u +%FT%TZ) ALL DONE matrix-batch-smoke.txt" >> $RUNS/summary.txt
