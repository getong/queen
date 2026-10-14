#!/usr/bin/env bash
# run.sh b1-1x200-1000k: p2 (generated 2026-10-13T10:59:54Z); pid in p2.pid, exit code in p2.rc
cd /root/bench/runs/queen-kafka/b1-1x200-1000k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x200-1000k -partitions 200 -entities 0 -create=false -rate 111111 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/queen-kafka/b1-1x200-1000k/p2.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 22 -cons-offset 44 -cons-total 198 -poll 1000 -ack async -loader-index 2 -loaders 9 -local-src 0-2 -out /root/bench/runs/queen-kafka/b1-1x200-1000k/p2.json -tag b1-1x200-1000k-p2 -group-protocol classic  > p2.log 2>&1 < /dev/null &
echo $! > p2.pid.tmp && mv p2.pid.tmp p2.pid
wait $!
echo $? > p2.rc.tmp && mv p2.rc.tmp p2.rc
