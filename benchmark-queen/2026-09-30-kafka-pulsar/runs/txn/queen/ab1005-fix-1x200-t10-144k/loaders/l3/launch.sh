#!/usr/bin/env bash
# txn/run.sh queen/ab1005-fix-1x200-t10-144k: start this host's load processes (idempotent: a lock per process)
cd /root/bench/runs/txn/queen/ab1005-fix-1x200-t10-144k || exit 1
mkdir -p ids
if mkdir p6.lock 2>/dev/null; then rm -f p6.start p6.rc p6.pid p6.json p6.log ids/p6.ids; nohup bash p6.sh > /dev/null 2>&1 < /dev/null & fi
if mkdir p7.lock 2>/dev/null; then rm -f p7.start p7.rc p7.pid p7.json p7.log ids/p7.ids; nohup bash p7.sh > /dev/null 2>&1 < /dev/null & fi
if mkdir p8.lock 2>/dev/null; then rm -f p8.start p8.rc p8.pid p8.json p8.log ids/p8.ids; nohup bash p8.sh > /dev/null 2>&1 < /dev/null & fi
echo launched 6 7 8  on $(hostname)
