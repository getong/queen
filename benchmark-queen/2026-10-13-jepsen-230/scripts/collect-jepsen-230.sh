#!/usr/bin/env bash
# collect-jepsen-230.sh: gather what the Jepsen archive of 2.3.0 keeps into /root/jepsen-230-archive (summaries,
# lists, checksums, notes, the scripts that ran, each test's results.edn; not the stores).
set -u
OUT=/root/jepsen-230-archive; rm -rf $OUT; mkdir -p $OUT/scripts
grab() { # grab <runs dir> <name>
  local RUNS=$1 D=$OUT/$2; mkdir -p $D/results
  cp $RUNS/summary.txt $D/summary.txt
  for f in binary.md5 reanalyze.txt notes.txt summary-smoke.txt summary-setup-check.txt; do [ -e $RUNS/$f ] && cp $RUNS/$f $D/; done
  grep " done " $RUNS/summary.txt | while read -r _ _ label rest; do
    label=${label%:}
    store=$(echo "$rest" | sed -n 's/.*store=\([^ ]*\) log=.*/\1/p')
    [ -n "$store" ] && [ -e "$store/results.edn" ] && cp "$store/results.edn" "$D/results/$label.edn"
    [ -n "$store" ] && [ -e "$store/results-unknown-2600m.edn" ] && cp "$store/results-unknown-2600m.edn" "$D/results/$label.unknown-2600m.edn"
  done
  echo "$2: $(grep -c " done " $D/summary.txt) done, $(ls $D/results | grep -c -v unknown) results, $(grep " done " $D/summary.txt | grep -vc ": valid") not valid in summary.txt"
}
grab /root/runs-230 main
grab /root/runs-230-fo failover
cp /root/jepsen-230/matrix-230-full.txt $OUT/main/matrix.txt
cp /root/jepsen-230/matrix-230-rest.txt $OUT/main/matrix-after-resize.txt
cp /root/jepsen-230/matrix-220b2.txt $OUT/failover/matrix.txt
cp /root/jepsen-230/campaign-230.sh /root/jepsen-230/campaign-230-fo.sh /root/jepsen-230/resume-230.sh /root/reanalyze2.sh $OUT/scripts/
cp /root/jepsen-230/project.clj $OUT/scripts/project.clj.as-run
md5sum /root/bin/queen-230b2 > $OUT/binaries.md5
du -sh $OUT
