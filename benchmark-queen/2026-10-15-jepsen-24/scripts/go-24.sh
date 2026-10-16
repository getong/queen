#!/usr/bin/env bash
# go-24.sh: the setup check, then (only if it is valid) the main list on n1..n5 and, queued behind it, the failover list on n1..n6.
cd /root/jepsen-24
./smoke-24.sh > /root/runs-24-smoke.out 2>&1
if grep -q "done  s-w8-none: valid" /root/runs-24-smoke/summary.txt; then
  mkdir -p /root/runs-24
  (setsid nohup ./campaign-24-fo.sh > /root/runs-24-fo.out 2>&1 < /dev/null &)
  ./campaign-24.sh > /root/runs-24.out 2>&1
else
  echo "$(date -u +%FT%TZ) SETUP CHECK NOT VALID: campaign not started" >> /root/runs-24-smoke/summary.txt
fi
