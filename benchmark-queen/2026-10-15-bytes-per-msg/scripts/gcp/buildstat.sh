#!/bin/bash
# Where the fixed-broker build on broker 1 is.
. "$(dirname "$0")/hosts.sh"
$SSH root@${BP[0]} 'tail -c 600 /root/fix-build.log; echo; uptime; ps -eo pid,etime,pcpu,comm --sort=-pcpu | head -5'
