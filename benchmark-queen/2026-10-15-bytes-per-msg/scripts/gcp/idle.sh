#!/bin/bash
# Before deleting the VMs: does anything of ours still run on them (queen, qload, ld.sh, mrun's samplers)?
. "$(dirname "$0")/hosts.sh"
for h in "${BP[@]}" "${LP[@]}"; do
  ( echo "$h: $($SSH root@$h 'n=$(pgrep -c -x queen); l=$(pgrep -c -x qload); s=$(pgrep -fc "netdisk|sampler" ); echo "queen=$n qload=$l samplers=$s load=$(cut -d" " -f1 /proc/loadavg)"' 2>&1)" ) &
done; wait
