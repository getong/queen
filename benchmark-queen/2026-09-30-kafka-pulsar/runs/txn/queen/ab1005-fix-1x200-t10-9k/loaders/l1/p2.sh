#!/usr/bin/env bash
# txn/run.sh queen/ab1005-fix-1x200-t10-9k: p2 (generated 2026-10-05T11:51:37Z); pid in p2.pid, exit code in p2.rc
cd /root/bench/runs/txn/queen/ab1005-fix-1x200-t10-9k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/txn/qload -urls http://10.114.0.9:6632,http://10.114.0.10:6632,http://10.114.0.11:6632 -pop-width 10 -pop-batch 1000 -lease 10 -topic xab1005-fix-1x200-t10-9k -partitions 200 -txn -txn-size 10 -txn-linger 1s -payload 256 -create=false -rate 1000 -ramp 10s -duration 70s -report 10s -drain 60s -idle-exit 3s -start-file /root/bench/runs/txn/queen/ab1005-fix-1x200-t10-9k/p2.start -max-inflight 5000 -batch 10 -consumers 22 -cons-offset 44 -cons-total 198 -loader-index 2 -loaders 9 -local-src 0-2 -out /root/bench/runs/txn/queen/ab1005-fix-1x200-t10-9k/p2.json -ids-out /root/bench/runs/txn/queen/ab1005-fix-1x200-t10-9k/ids/p2.ids -tag ab1005-fix-1x200-t10-9k-p2  > p2.log 2>&1 < /dev/null &
echo $! > p2.pid.tmp && mv p2.pid.tmp p2.pid
wait $!
echo $? > p2.rc.tmp && mv p2.rc.tmp p2.rc
