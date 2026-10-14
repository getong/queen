#!/usr/bin/env bash
# run.sh b1-1x200-2000k: p8 (generated 2026-10-13T11:10:15Z); pid in p8.pid, exit code in p8.rc
cd /root/bench/runs/queen-kafka/b1-1x200-2000k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x200-2000k -partitions 200 -entities 0 -create=false -rate 222222 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/queen-kafka/b1-1x200-2000k/p8.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 22 -cons-offset 176 -cons-total 198 -poll 1000 -ack async -loader-index 8 -loaders 9 -local-src 6-8 -out /root/bench/runs/queen-kafka/b1-1x200-2000k/p8.json -tag b1-1x200-2000k-p8 -group-protocol classic  > p8.log 2>&1 < /dev/null &
echo $! > p8.pid.tmp && mv p8.pid.tmp p8.pid
wait $!
echo $? > p8.rc.tmp && mv p8.rc.tmp p8.rc
