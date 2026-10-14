#!/usr/bin/env bash
# run.sh 1x100-4000k-oct13: p3 (generated 2026-10-13T10:14:17Z); pid in p3.pid, exit code in p3.rc
cd /root/bench/runs/kafka/1x100-4000k-oct13 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic k1x100-4000k-oct13 -partitions 100 -entities 0 -create=false -rate 444444 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x100-4000k-oct13/p3.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 11 -cons-offset 33 -cons-total 99 -poll 1000 -ack async -loader-index 3 -loaders 9 -local-src 3-5 -out /root/bench/runs/kafka/1x100-4000k-oct13/p3.json -tag 1x100-4000k-oct13-p3  > p3.log 2>&1 < /dev/null &
echo $! > p3.pid.tmp && mv p3.pid.tmp p3.pid
wait $!
echo $? > p3.rc.tmp && mv p3.rc.tmp p3.rc
