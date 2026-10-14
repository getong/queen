#!/usr/bin/env bash
# run.sh 1x1000-1000k-p1k-gcp24-lazy: p5 (generated 2026-10-14T14:38:40Z); pid in p5.pid, exit code in p5.rc
cd /root/bench/runs/kafka/1x1000-1000k-p1k-gcp24-lazy || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x1000-1000k-p1k-gcp24-lazy -partitions 1000 -entities 0 -create=false -rate 111111 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x1000-1000k-p1k-gcp24-lazy/p5.start -max-inflight 5000 -mode batch -payload 1024 -dist rr -consumers 33 -cons-offset 165 -cons-total 297 -poll 1000 -ack async -loader-index 5 -loaders 9 -local-src 3-5 -out /root/bench/runs/kafka/1x1000-1000k-p1k-gcp24-lazy/p5.json -tag 1x1000-1000k-p1k-gcp24-lazy-p5  > p5.log 2>&1 < /dev/null &
echo $! > p5.pid.tmp && mv p5.pid.tmp p5.pid
wait $!
echo $? > p5.rc.tmp && mv p5.rc.tmp p5.rc
