#!/usr/bin/env bash
# run.sh 1x50000-3000k-gcp24-xfs-t24: p6 (generated 2026-10-14T16:01:12Z); pid in p6.pid, exit code in p6.rc
cd /root/bench/runs/kafka/1x50000-3000k-gcp24-xfs-t24 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400 GOMEMLIMIT=9GiB
/root/bench/bin/kload -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x50000-3000k-gcp24-xfs-t24 -partitions 50000 -entities 0 -create=false -rate 333333 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x50000-3000k-gcp24-xfs-t24/p6.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 198 -cons-total 297 -poll 1000 -ack async -loader-index 6 -loaders 9 -local-src 6-8 -out /root/bench/runs/kafka/1x50000-3000k-gcp24-xfs-t24/p6.json -tag 1x50000-3000k-gcp24-xfs-t24-p6 -group-timeout 900s  > p6.log 2>&1 < /dev/null &
echo $! > p6.pid.tmp && mv p6.pid.tmp p6.pid
wait $!
echo $? > p6.rc.tmp && mv p6.rc.tmp p6.rc
