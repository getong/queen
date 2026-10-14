#!/usr/bin/env bash
# run.sh 1x50000-1000k-p1k-gcp24-xfs-t24-r: p0 (generated 2026-10-14T16:11:48Z); pid in p0.pid, exit code in p0.rc
cd /root/bench/runs/kafka/1x50000-1000k-p1k-gcp24-xfs-t24-r || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400 GOMEMLIMIT=9GiB
/root/bench/bin/kload -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x50000-1000k-p1k-gcp24-xfs-t24-r -partitions 50000 -entities 0 -create=false -rate 111111 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x50000-1000k-p1k-gcp24-xfs-t24-r/p0.start -max-inflight 5000 -mode batch -payload 1024 -dist rr -consumers 33 -cons-offset 0 -cons-total 297 -poll 1000 -ack async -loader-index 0 -loaders 9 -local-src 0-2 -out /root/bench/runs/kafka/1x50000-1000k-p1k-gcp24-xfs-t24-r/p0.json -tag 1x50000-1000k-p1k-gcp24-xfs-t24-r-p0 -group-timeout 900s  > p0.log 2>&1 < /dev/null &
echo $! > p0.pid.tmp && mv p0.pid.tmp p0.pid
wait $!
echo $? > p0.rc.tmp && mv p0.rc.tmp p0.rc
