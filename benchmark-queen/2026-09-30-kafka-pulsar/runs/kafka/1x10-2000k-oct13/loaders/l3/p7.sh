#!/usr/bin/env bash
# run.sh 1x10-2000k-oct13: p7 (generated 2026-10-13T10:17:25Z); pid in p7.pid, exit code in p7.rc
cd /root/bench/runs/kafka/1x10-2000k-oct13 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic k1x10-2000k-oct13 -partitions 10 -entities 0 -create=false -rate 222222 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x10-2000k-oct13/p7.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 1 -cons-offset 7 -cons-total 9 -poll 1000 -ack async -loader-index 7 -loaders 9 -local-src 6-8 -out /root/bench/runs/kafka/1x10-2000k-oct13/p7.json -tag 1x10-2000k-oct13-p7  > p7.log 2>&1 < /dev/null &
echo $! > p7.pid.tmp && mv p7.pid.tmp p7.pid
wait $!
echo $? > p7.rc.tmp && mv p7.rc.tmp p7.rc
