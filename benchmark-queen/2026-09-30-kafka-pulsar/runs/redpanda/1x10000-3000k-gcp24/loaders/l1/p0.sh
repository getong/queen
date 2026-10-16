#!/usr/bin/env bash
# run.sh 1x10000-3000k-gcp24: p0 (generated 2026-10-16T07:21:11Z); pid in p0.pid, exit code in p0.rc
cd /root/bench/runs/redpanda/1x10000-3000k-gcp24 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400 GOMEMLIMIT=9GiB
/root/bench/bin/kload -brokers 10.250.0.23:9092,10.250.0.22:9092,10.250.0.25:9092 -topics 1 -topic r1x10000-3000k-gcp24 -partitions 10000 -entities 0 -create=false -rate 333333 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/redpanda/1x10000-3000k-gcp24/p0.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 0 -cons-total 297 -poll 1000 -ack async -loader-index 0 -loaders 9 -local-src 0-2 -out /root/bench/runs/redpanda/1x10000-3000k-gcp24/p0.json -tag 1x10000-3000k-gcp24-p0 -group-protocol classic -min-isr 0  > p0.log 2>&1 < /dev/null &
echo $! > p0.pid.tmp && mv p0.pid.tmp p0.pid
wait $!
echo $? > p0.rc.tmp && mv p0.rc.tmp p0.rc
