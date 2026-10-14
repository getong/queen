#!/usr/bin/env bash
# run.sh b1-1x200-300k: p0 (generated 2026-10-13T10:50:04Z); pid in p0.pid, exit code in p0.rc
cd /root/bench/runs/queen-kafka/b1-1x200-300k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x200-300k -partitions 200 -entities 0 -create=false -rate 33333 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/queen-kafka/b1-1x200-300k/p0.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 22 -cons-offset 0 -cons-total 198 -poll 1000 -ack async -loader-index 0 -loaders 9 -local-src 0-2 -out /root/bench/runs/queen-kafka/b1-1x200-300k/p0.json -tag b1-1x200-300k-p0  > p0.log 2>&1 < /dev/null &
echo $! > p0.pid.tmp && mv p0.pid.tmp p0.pid
wait $!
echo $? > p0.rc.tmp && mv p0.rc.tmp p0.rc
