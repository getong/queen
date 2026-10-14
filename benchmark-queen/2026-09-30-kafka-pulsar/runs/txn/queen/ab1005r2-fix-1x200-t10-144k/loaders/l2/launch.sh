#!/usr/bin/env bash
# txn/run.sh queen/ab1005r2-fix-1x200-t10-144k: start this host's load processes (idempotent: a lock per process)
cd /root/bench/runs/txn/queen/ab1005r2-fix-1x200-t10-144k || exit 1
mkdir -p ids
if mkdir p3.lock 2>/dev/null; then rm -f p3.start p3.rc p3.pid p3.json p3.log ids/p3.ids; nohup bash p3.sh > /dev/null 2>&1 < /dev/null & fi
if mkdir p4.lock 2>/dev/null; then rm -f p4.start p4.rc p4.pid p4.json p4.log ids/p4.ids; nohup bash p4.sh > /dev/null 2>&1 < /dev/null & fi
if mkdir p5.lock 2>/dev/null; then rm -f p5.start p5.rc p5.pid p5.json p5.log ids/p5.ids; nohup bash p5.sh > /dev/null 2>&1 < /dev/null & fi
echo launched 3 4 5  on $(hostname)
