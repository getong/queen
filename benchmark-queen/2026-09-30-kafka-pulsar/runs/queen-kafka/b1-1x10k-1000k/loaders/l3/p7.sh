#!/usr/bin/env bash
# run.sh b1-1x10k-1000k: p7 (generated 2026-10-13T11:06:43Z); pid in p7.pid, exit code in p7.rc
cd /root/bench/runs/queen-kafka/b1-1x10k-1000k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x10k-1000k -partitions 10000 -entities 0 -create=false -rate 111111 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/queen-kafka/b1-1x10k-1000k/p7.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 231 -cons-total 297 -poll 1000 -ack async -loader-index 7 -loaders 9 -local-src 6-8 -out /root/bench/runs/queen-kafka/b1-1x10k-1000k/p7.json -tag b1-1x10k-1000k-p7 -group-protocol classic  > p7.log 2>&1 < /dev/null &
echo $! > p7.pid.tmp && mv p7.pid.tmp p7.pid
wait $!
echo $? > p7.rc.tmp && mv p7.rc.tmp p7.rc
