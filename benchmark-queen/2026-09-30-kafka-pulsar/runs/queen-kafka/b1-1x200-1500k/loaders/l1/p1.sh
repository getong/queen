#!/usr/bin/env bash
# run.sh b1-1x200-1500k: p1 (generated 2026-10-13T11:03:28Z); pid in p1.pid, exit code in p1.rc
cd /root/bench/runs/queen-kafka/b1-1x200-1500k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x200-1500k -partitions 200 -entities 0 -create=false -rate 166666 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/queen-kafka/b1-1x200-1500k/p1.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 22 -cons-offset 22 -cons-total 198 -poll 1000 -ack async -loader-index 1 -loaders 9 -local-src 0-2 -out /root/bench/runs/queen-kafka/b1-1x200-1500k/p1.json -tag b1-1x200-1500k-p1 -group-protocol classic  > p1.log 2>&1 < /dev/null &
echo $! > p1.pid.tmp && mv p1.pid.tmp p1.pid
wait $!
echo $? > p1.rc.tmp && mv p1.rc.tmp p1.rc
