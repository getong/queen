#!/bin/bash
# Brokers: RAID0 of the four local NVMe disks, XFS (mkfs defaults), mounted noatime at /root/p3 (the harness ROOT),
# as on 10-14 after 15:08Z. Not in fstab: a stop loses it.
. "$(dirname "$0")/hosts.sh"
SETUP='set -e
export DEBIAN_FRONTEND=noninteractive
if ! command -v mdadm >/dev/null || ! command -v mkfs.xfs >/dev/null; then
  apt-get update -qq >/dev/null && apt-get install -y -qq mdadm xfsprogs >/dev/null
fi
if ! mountpoint -q /root/p3; then
  DEVS=$(ls /dev/disk/by-id/google-local-nvme-ssd-* | sort)
  mdadm --create /dev/md0 --level=0 --raid-devices=4 --run $DEVS >/dev/null 2>&1
  mkfs.xfs -f -q /dev/md0
  mkdir -p /root/p3 && mount -o noatime /dev/md0 /root/p3
fi
fstrim /root/p3 || true
echo "$(hostname): $(findmnt -no SOURCE,FSTYPE,OPTIONS /root/p3 | cut -c1-60) $(df -h --output=size /root/p3 | tail -1)"'
for h in "${BP[@]}"; do $SSH root@$h "$SETUP" & done; wait
