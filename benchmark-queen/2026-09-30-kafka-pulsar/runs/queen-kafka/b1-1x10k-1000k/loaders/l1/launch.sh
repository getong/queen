#!/usr/bin/env bash
# run.sh b1-1x10k-1000k: start this host's load processes (generated 2026-10-13T11:06:37Z)
cd /root/bench/runs/queen-kafka/b1-1x10k-1000k || exit 1
rm -f p0.start p0.rc p0.pid p0.json p0.log; nohup bash p0.sh > /dev/null 2>&1 < /dev/null &
rm -f p1.start p1.rc p1.pid p1.json p1.log; nohup bash p1.sh > /dev/null 2>&1 < /dev/null &
rm -f p2.start p2.rc p2.pid p2.json p2.log; nohup bash p2.sh > /dev/null 2>&1 < /dev/null &
echo launched 0 1 2  on $(hostname)
