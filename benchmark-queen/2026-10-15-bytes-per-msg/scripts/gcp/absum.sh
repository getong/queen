#!/bin/bash
# Per run of the A/B: the step line (pushed, latency, cores) and each broker's disk bytes per message over the
# step's seconds 30-60 (net-n*.log lines 30-60, as for yesterday's cs-p1000-r3000000).
# The four runs were moved to ../../runs/gcp after the A/B; yesterday's stock run is untracked in the main checkout
# (Y= names it).
R=$(cd "$(dirname "$0")/../../runs/gcp" && pwd)
Y=${Y:-$(cd "$(dirname "$0")/../../.." && pwd)/2026-10-12-three-node-new-forms/runs/cs-p1000-r3000000}
N=$(cd "$(dirname "$0")/.." && pwd)/netdisk_rate.py
show() {
  local dir=$1 name=$2
  echo "== $name"
  grep -h '3,000,000:' $dir/run.log | sed 's/^.*3,000,000: //' | cut -c1-260
  python3 $N 3000000 30 60 $dir/3000000/net-n1.log $dir/3000000/net-n2.log $dir/3000000/net-n3.log 2>/dev/null | sed 's#^.*/net-#  #'
}
show $Y "yesterday cs-p1000-r3000000 (stock 2.3.0)"
for t in bpm1-230-p1000 bpm1-fix-p1000 bpm2-230-p1000 bpm2-fix-p1000; do
  [ -d $R/$t/3000000 ] && show $R/$t $t
done
