#!/bin/bash
# Forget old host keys for these reused addresses, then wait until root ssh answers on all six.
. "$(dirname "$0")/hosts.sh"
for h in "${ALL[@]}"; do ssh-keygen -R "$h" >/dev/null 2>&1; done
for h in "${ALL[@]}"; do
  ok=0
  for t in $(seq 1 40); do
    if $SSH root@$h 'echo up' >/dev/null 2>&1; then ok=1; break; fi
    sleep 5
  done
  echo "$h ssh=$ok $($SSH root@$h 'nproc; free -g | awk "/Mem/{print \$2\"G\"}"; ls /dev/disk/by-id/ | grep -c google-local-nvme-ssd' 2>/dev/null | tr '\n' ' ')"
done
