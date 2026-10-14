#!/usr/bin/env bash
# run.sh 1x100000-1000k-p1k-gcp24-lazy: p4 (generated 2026-10-14T14:48:56Z); pid in p4.pid, exit code in p4.rc
cd /root/bench/runs/kafka/1x100000-1000k-p1k-gcp24-lazy || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400 GOMEMLIMIT=9GiB
/root/bench/bin/kload -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x100000-1000k-p1k-gcp24-lazy -partitions 100000 -entities 0 -create=false -rate 111111 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x100000-1000k-p1k-gcp24-lazy/p4.start -max-inflight 5000 -mode batch -payload 1024 -dist rr -consumers 33 -cons-offset 132 -cons-total 297 -poll 1000 -ack async -loader-index 4 -loaders 9 -local-src 3-5 -out /root/bench/runs/kafka/1x100000-1000k-p1k-gcp24-lazy/p4.json -tag 1x100000-1000k-p1k-gcp24-lazy-p4 -group-timeout 900s  > p4.log 2>&1 < /dev/null &
echo $! > p4.pid.tmp && mv p4.pid.tmp p4.pid
wait $!
echo $? > p4.rc.tmp && mv p4.rc.tmp p4.rc
