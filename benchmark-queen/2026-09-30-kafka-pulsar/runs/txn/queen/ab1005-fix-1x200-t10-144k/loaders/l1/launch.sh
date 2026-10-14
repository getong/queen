#!/usr/bin/env bash
# txn/run.sh queen/ab1005-fix-1x200-t10-144k: start this host's load processes (idempotent: a lock per process)
cd /root/bench/runs/txn/queen/ab1005-fix-1x200-t10-144k || exit 1
mkdir -p ids
if mkdir p0.lock 2>/dev/null; then rm -f p0.start p0.rc p0.pid p0.json p0.log ids/p0.ids; nohup bash p0.sh > /dev/null 2>&1 < /dev/null & fi
if mkdir p1.lock 2>/dev/null; then rm -f p1.start p1.rc p1.pid p1.json p1.log ids/p1.ids; nohup bash p1.sh > /dev/null 2>&1 < /dev/null & fi
if mkdir p2.lock 2>/dev/null; then rm -f p2.start p2.rc p2.pid p2.json p2.log ids/p2.ids; nohup bash p2.sh > /dev/null 2>&1 < /dev/null & fi
echo launched 0 1 2  on $(hostname)
