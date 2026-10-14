#!/usr/bin/env bash
# txn/run.sh queen/ab1005r2-fix-1x200-t10-144k: create (generated 2026-10-05T12:03:10Z); pid in create.pid, exit code in create.rc
cd /root/bench/runs/txn/queen/ab1005r2-fix-1x200-t10-144k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/txn/qload -urls http://10.114.0.9:6632,http://10.114.0.10:6632,http://10.114.0.11:6632 -pop-width 10 -pop-batch 1000 -lease 10 -topic xab1005r2-fix-1x200-t10-144k -partitions 200 -txn -txn-size 10 -txn-linger 1s -payload 256 -create-only -warm -tag ab1005r2-fix-1x200-t10-144k-create -out /root/bench/runs/txn/queen/ab1005r2-fix-1x200-t10-144k/create.json  > create.log 2>&1 < /dev/null &
echo $! > create.pid.tmp && mv create.pid.tmp create.pid
wait $!
echo $? > create.rc.tmp && mv create.rc.tmp create.rc
