#!/usr/bin/env bash
# run.sh 1x1000-5000k-gcp24: p2 (generated 2026-10-16T10:18:14Z); pid in p2.pid, exit code in p2.rc
cd /root/bench/runs/redpanda/1x1000-5000k-gcp24 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400 GOMEMLIMIT=9GiB
/root/bench/bin/kload -brokers 10.250.0.23:9092,10.250.0.22:9092,10.250.0.25:9092 -topics 1 -topic r1x1000-5000k-gcp24 -partitions 1000 -entities 0 -create=false -rate 555555 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/redpanda/1x1000-5000k-gcp24/p2.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 66 -cons-total 297 -poll 1000 -ack async -loader-index 2 -loaders 9 -local-src 0-2 -out /root/bench/runs/redpanda/1x1000-5000k-gcp24/p2.json -tag 1x1000-5000k-gcp24-p2 -group-protocol classic -min-isr 0  > p2.log 2>&1 < /dev/null &
echo $! > p2.pid.tmp && mv p2.pid.tmp p2.pid
wait $!
echo $? > p2.rc.tmp && mv p2.rc.tmp p2.rc
