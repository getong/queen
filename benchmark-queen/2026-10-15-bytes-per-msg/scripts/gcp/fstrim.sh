#!/bin/bash
# fstrim of each broker's RAID before a series (XFS has no discard: wiped runs leave the SSDs untrimmed; one SSD
# slowed to 30 ms a write after a day of runs on 2026-10-14). Skipped on a broker where queen still runs.
. "$(dirname "$0")/hosts.sh"
for i in 0 1 2; do
  ( out=$($SSH root@${BP[$i]} 'pgrep -x queen >/dev/null && echo "queen RUNNING, no trim" && exit 0; fstrim -v /root/p3 2>&1 | tail -1')
    echo "n$((i+1)): $out" ) &
done; wait
