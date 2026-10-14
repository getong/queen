#!/usr/bin/env bash
# run.sh b1-1x200-1000k: p4 (generated 2026-10-13T10:59:58Z); pid in p4.pid, exit code in p4.rc
cd /root/bench/runs/queen-kafka/b1-1x200-1000k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x200-1000k -partitions 200 -entities 0 -create=false -rate 111111 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/queen-kafka/b1-1x200-1000k/p4.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 22 -cons-offset 88 -cons-total 198 -poll 1000 -ack async -loader-index 4 -loaders 9 -local-src 3-5 -out /root/bench/runs/queen-kafka/b1-1x200-1000k/p4.json -tag b1-1x200-1000k-p4 -group-protocol classic  > p4.log 2>&1 < /dev/null &
echo $! > p4.pid.tmp && mv p4.pid.tmp p4.pid
wait $!
echo $? > p4.rc.tmp && mv p4.rc.tmp p4.rc
