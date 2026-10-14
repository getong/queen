#!/usr/bin/env bash
# run.sh 1x100000-3000k-gcp24-lazy: p3 (generated 2026-10-14T14:27:18Z); pid in p3.pid, exit code in p3.rc
cd /root/bench/runs/kafka/1x100000-3000k-gcp24-lazy || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400 GOMEMLIMIT=9GiB
/root/bench/bin/kload -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x100000-3000k-gcp24-lazy -partitions 100000 -entities 0 -create=false -rate 333333 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x100000-3000k-gcp24-lazy/p3.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 99 -cons-total 297 -poll 1000 -ack async -loader-index 3 -loaders 9 -local-src 3-5 -out /root/bench/runs/kafka/1x100000-3000k-gcp24-lazy/p3.json -tag 1x100000-3000k-gcp24-lazy-p3 -group-timeout 900s  > p3.log 2>&1 < /dev/null &
echo $! > p3.pid.tmp && mv p3.pid.tmp p3.pid
wait $!
echo $? > p3.rc.tmp && mv p3.rc.tmp p3.rc
