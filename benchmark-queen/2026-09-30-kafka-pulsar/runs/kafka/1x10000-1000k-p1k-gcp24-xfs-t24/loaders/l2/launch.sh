#!/usr/bin/env bash
# run.sh 1x10000-1000k-p1k-gcp24-xfs-t24: start this host's load processes (generated 2026-10-14T15:31:45Z)
cd /root/bench/runs/kafka/1x10000-1000k-p1k-gcp24-xfs-t24 || exit 1
rm -f p3.start p3.rc p3.pid p3.json p3.log; nohup bash p3.sh > /dev/null 2>&1 < /dev/null &
rm -f p4.start p4.rc p4.pid p4.json p4.log; nohup bash p4.sh > /dev/null 2>&1 < /dev/null &
rm -f p5.start p5.rc p5.pid p5.json p5.log; nohup bash p5.sh > /dev/null 2>&1 < /dev/null &
echo launched 3 4 5  on $(hostname)
