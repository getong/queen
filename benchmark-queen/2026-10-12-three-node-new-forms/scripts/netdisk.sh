#!/usr/bin/env bash
# Once a second: wall time, bytes received and sent on the private interface, sectors written to the root disk.
. /root/p3/node.env
IF=$(ip -o -4 addr show | awk -v a="$PRIV/" 'index($4, a) == 1 {print $2; exit}')
DEV=$(lsblk -no PKNAME "$(findmnt -no SOURCE /)" 2>/dev/null | head -1); DEV=${DEV:-vda}
while true; do
  echo "t=$(date +%s.%N) rx=$(cat /sys/class/net/$IF/statistics/rx_bytes) tx=$(cat /sys/class/net/$IF/statistics/tx_bytes) wsec=$(awk '{print $7}' /sys/block/$DEV/stat)"
  sleep 1
done
