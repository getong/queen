#!/usr/bin/env bash
# The three log-workload histories whose checker ran out of memory at 2.6 GB, checked again with 8 GB, at the
# lowest CPU priority beside the running campaign (the clients of its tests keep the CPU first). Runs from a copy
# of the harness whose project.clj asks for 8 GB, with its store linked to the campaign's store.
set -u
rm -rf /root/jepsen-230-analyze
mkdir -p /root/jepsen-230-analyze
cp -r /root/jepsen-230/project.clj /root/jepsen-230/src /root/jepsen-230-analyze/
[ -d /root/jepsen-230/test ] && cp -r /root/jepsen-230/test /root/jepsen-230-analyze/
ln -s /root/jepsen-230/store /root/jepsen-230-analyze/store
cd /root/jepsen-230-analyze
sed -i 's/"-Xmx12g"/"-Xmx8g"/' project.clj
OUT=/root/runs-230/reanalyze.txt
for t in rw-w1-part p11-w1-part p11-w1-leader-deaf; do
  store=$(grep "done  $t:" /root/runs-230/summary.txt | sed -n 's/.* store=\([^ ]*\) .*/\1/p' | tail -1)
  echo "$(date -u +%FT%TZ) start $t (second try: the folder, 8 GB, nice 19): $store" >> $OUT
  log=/root/runs-230/$t-reanalyze2.log
  nice -n 19 lein run analyze -t "$store" > "$log" 2>&1; rc=$?
  if grep -q "Everything looks good" "$log"; then v=valid
  elif grep -q "Analysis invalid" "$log"; then v=INVALID
  elif grep -q "Errors occurred during analysis" "$log"; then v=UNKNOWN
  else v="CRASHED(rc=$rc)"; fi
  echo "$(date -u +%FT%TZ) done  $t: $v log=$log" >> $OUT
done
echo "$(date -u +%FT%TZ) REANALYZE2 DONE" >> $OUT
