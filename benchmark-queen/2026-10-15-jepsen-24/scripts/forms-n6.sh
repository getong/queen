#!/usr/bin/env bash
# After each smoke test that leaves its broker running: n6 queue logs and checkpoint, as bpm reads them.
for t in bs-w1-none bs-w1-kill; do
  until grep -q "done  $t:" /root/runs-batch-smoke/summary.txt 2>/dev/null; do sleep 1; done
  echo "== $t $(date -u +%T)"
  ssh -n n6 "/root/bpm parse /opt/queen/data 2>&1 | grep -E \"^files|tag/version|effects by kind\"; for m in \$(find /opt/queen/data -name data.mdb); do /root/bpm lmdb \$m | grep -E \"^values (txns|request_ids)\"; done"
done
