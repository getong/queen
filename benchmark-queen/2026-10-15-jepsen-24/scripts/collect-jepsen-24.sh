#!/usr/bin/env bash
# collect-jepsen-24.sh: gather what the Jepsen archive of 2.4 keeps into /root/jepsen-24-archive (summaries, lists,
# checksums, notes, the scripts that ran, each test's results.edn, the setup checks, the rolling upgrade; not the stores).
set -u
OUT=/root/jepsen-24-archive; rm -rf $OUT; mkdir -p $OUT/scripts $OUT/upgrade
grab() { # grab <runs dir> <name>
  local RUNS=$1 D=$OUT/$2; [ -e $RUNS/summary.txt ] || return 0; mkdir -p $D/results
  cp $RUNS/summary.txt $D/summary.txt
  for f in binary.md5 reanalyze.txt notes.txt cv.txt; do [ -e $RUNS/$f ] && cp $RUNS/$f $D/; done
  grep " done " $RUNS/summary.txt | while read -r _ _ label rest; do
    label=${label%:}
    store=$(echo "$rest" | sed -n 's/.*store=\([^ ]*\) log=.*/\1/p')
    [ -n "$store" ] && [ -e "$store/results.edn" ] && cp "$store/results.edn" "$D/results/$label.edn"
  done
  echo "$2: $(grep -c " done " $D/summary.txt) done, $(ls $D/results | wc -l) results, $(grep " done " $D/summary.txt | grep -vc ": valid") not valid in summary.txt"
}
grab /root/runs-24-smoke setup-check
grab /root/runs-24 main
grab /root/runs-24-fo failover
grab /root/runs-24-batch batch
grab /root/runs-batch-smoke batch-smoke
cp /root/jepsen-24/matrix-24.txt $OUT/main/matrix.txt
cp /root/jepsen-24/matrix-220b2.txt $OUT/failover/matrix.txt
cp /root/jepsen-24/matrix-smoke-24.txt $OUT/setup-check/matrix.txt
[ -d $OUT/batch ] && cp /root/jepsen-batch/matrix-24-batch.txt $OUT/batch/matrix.txt
[ -d $OUT/batch-smoke ] && cp /root/jepsen-batch/matrix-batch-smoke.txt $OUT/batch-smoke/matrix.txt && cp /root/forms-n6.out $OUT/batch-smoke/forms-n6.txt
cp /root/jepsen-24/campaign-24.sh /root/jepsen-24/campaign-24-fo.sh /root/jepsen-24/smoke-24.sh /root/jepsen-24/go-24.sh /root/jepsen-24/collect-jepsen-24.sh $OUT/scripts/
cp /root/jepsen-batch/campaign-24-batch.sh /root/jepsen-batch/smoke-batch.sh /root/forms-n6.sh $OUT/scripts/
cp /root/jepsen-24/project.clj $OUT/scripts/project.clj.as-run
cp /root/roll/roll24.sh /root/roll/node.sh /root/roll/dup.py /root/roll/health.py /root/roll/runs/roll24/roll.log $OUT/upgrade/
md5sum /root/bin/queen-24 /root/bin/queen-230 > $OUT/binaries.md5
du -sh $OUT
