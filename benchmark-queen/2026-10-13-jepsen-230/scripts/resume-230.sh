#!/usr/bin/env bash
# After the resize of the control to 16 GB (2026-10-14 05:18Z), in this order, one thing at a time:
# 1. the three log-workload tests whose checker ran out of memory at 2.6 GB are checked again on their stored
#    histories (lein run analyze, 12 GB); their first results stay as results-unknown-2600m.edn;
# 2. the 37 tests the stop for the resize left (matrix-230-rest.txt);
# 3. the 31 failover tests on n1..n6 (matrix-220b2.txt) into /root/runs-230-fo.
cd /root/jepsen-230
OUT=/root/runs-230/reanalyze.txt
for t in rw-w1-part p11-w1-part p11-w1-leader-deaf; do
  store=$(grep "done  $t:" /root/runs-230/summary.txt | sed -n "s/.* store=\([^ ]*\) .*/\1/p" | tail -1)
  [ -f "$store/results-unknown-2600m.edn" ] || cp "$store/results.edn" "$store/results-unknown-2600m.edn"
  echo "$(date -u +%FT%TZ) start $t: $store" >> $OUT
  log=/root/runs-230/$t-reanalyze.log
  lein run analyze -t "$store/test.jepsen" > "$log" 2>&1; rc=$?
  if grep -q "Everything looks good" "$log"; then v=valid
  elif grep -q "Analysis invalid" "$log"; then v=INVALID
  elif grep -q "Errors occurred during analysis" "$log"; then v=UNKNOWN
  else v="CRASHED(rc=$rc)"; fi
  echo "$(date -u +%FT%TZ) done  $t: $v (re-analysis of the stored history) log=$log" >> $OUT
done
echo "$(date -u +%FT%TZ) REANALYZE DONE" >> $OUT
./campaign-230.sh matrix-230-rest.txt
export RUNS=/root/runs-230-fo BIN=/root/bin/queen-230b2 NODES=n1,n2,n3,n4,n5,n6
mkdir -p $RUNS; cp /root/runs-230/binary.md5 $RUNS/
./run-campaign.sh matrix-220b2.txt
echo "$(date -u +%FT%TZ) ALL DONE matrix-220b2.txt" >> $RUNS/summary.txt
