#!/bin/bash
# 32 KB write + fdatasync for 12 s on each broker's /root/p3 (md0), all three at once.
. "$(dirname "$0")/hosts.sh"
G=$(cd "$(dirname "$0")" && pwd)
for i in 0 1 2; do
  ( scp -q $G/fsync-md0.py root@${BP[$i]}:/root/fsync-md0.py; echo "n$((i+1)): $($SSH root@${BP[$i]} 'python3 /root/fsync-md0.py')" ) &
done; wait
