#!/usr/bin/env bash
# run.sh 1x100-3000k-gcp24-dbb1g-poll5k: p8 (generated 2026-10-14T12:54:35Z); pid in p8.pid, exit code in p8.rc
cd /root/bench/runs/kafka/1x100-3000k-gcp24-dbb1g-poll5k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x100-3000k-gcp24-dbb1g-poll5k -partitions 100 -entities 0 -create=false -rate 333333 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x100-3000k-gcp24-dbb1g-poll5k/p8.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 11 -cons-offset 88 -cons-total 99 -poll 5000 -ack async -loader-index 8 -loaders 9 -local-src 6-8 -out /root/bench/runs/kafka/1x100-3000k-gcp24-dbb1g-poll5k/p8.json -tag 1x100-3000k-gcp24-dbb1g-poll5k-p8 -fetch-max-partition-bytes 8388608  > p8.log 2>&1 < /dev/null &
echo $! > p8.pid.tmp && mv p8.pid.tmp p8.pid
wait $!
echo $? > p8.rc.tmp && mv p8.rc.tmp p8.rc
