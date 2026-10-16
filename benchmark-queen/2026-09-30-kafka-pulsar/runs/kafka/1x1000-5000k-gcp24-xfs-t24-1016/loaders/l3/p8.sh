#!/usr/bin/env bash
# run.sh 1x1000-5000k-gcp24-xfs-t24-1016: p8 (generated 2026-10-16T10:13:15Z); pid in p8.pid, exit code in p8.rc
cd /root/bench/runs/kafka/1x1000-5000k-gcp24-xfs-t24-1016 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.250.0.23:9092,10.250.0.22:9092,10.250.0.25:9092 -topics 1 -topic k1x1000-5000k-gcp24-xfs-t24-1016 -partitions 1000 -entities 0 -create=false -rate 555555 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x1000-5000k-gcp24-xfs-t24-1016/p8.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 264 -cons-total 297 -poll 1000 -ack async -loader-index 8 -loaders 9 -local-src 6-8 -out /root/bench/runs/kafka/1x1000-5000k-gcp24-xfs-t24-1016/p8.json -tag 1x1000-5000k-gcp24-xfs-t24-1016-p8  > p8.log 2>&1 < /dev/null &
echo $! > p8.pid.tmp && mv p8.pid.tmp p8.pid
wait $!
echo $? > p8.rc.tmp && mv p8.rc.tmp p8.rc
