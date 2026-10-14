#!/usr/bin/env bash
# run.sh b1-1x200-300k: p6 (generated 2026-10-13T10:50:11Z); pid in p6.pid, exit code in p6.rc
cd /root/bench/runs/queen-kafka/b1-1x200-300k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x200-300k -partitions 200 -entities 0 -create=false -rate 33333 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/queen-kafka/b1-1x200-300k/p6.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 22 -cons-offset 132 -cons-total 198 -poll 1000 -ack async -loader-index 6 -loaders 9 -local-src 6-8 -out /root/bench/runs/queen-kafka/b1-1x200-300k/p6.json -tag b1-1x200-300k-p6  > p6.log 2>&1 < /dev/null &
echo $! > p6.pid.tmp && mv p6.pid.tmp p6.pid
wait $!
echo $? > p6.rc.tmp && mv p6.rc.tmp p6.rc
