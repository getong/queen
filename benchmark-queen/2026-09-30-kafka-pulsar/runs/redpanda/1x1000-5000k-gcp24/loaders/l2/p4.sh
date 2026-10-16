#!/usr/bin/env bash
# run.sh 1x1000-5000k-gcp24: p4 (generated 2026-10-16T10:18:15Z); pid in p4.pid, exit code in p4.rc
cd /root/bench/runs/redpanda/1x1000-5000k-gcp24 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400 GOMEMLIMIT=9GiB
/root/bench/bin/kload -brokers 10.250.0.23:9092,10.250.0.22:9092,10.250.0.25:9092 -topics 1 -topic r1x1000-5000k-gcp24 -partitions 1000 -entities 0 -create=false -rate 555555 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/redpanda/1x1000-5000k-gcp24/p4.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 132 -cons-total 297 -poll 1000 -ack async -loader-index 4 -loaders 9 -local-src 3-5 -out /root/bench/runs/redpanda/1x1000-5000k-gcp24/p4.json -tag 1x1000-5000k-gcp24-p4 -group-protocol classic -min-isr 0  > p4.log 2>&1 < /dev/null &
echo $! > p4.pid.tmp && mv p4.pid.tmp p4.pid
wait $!
echo $? > p4.rc.tmp && mv p4.rc.tmp p4.rc
