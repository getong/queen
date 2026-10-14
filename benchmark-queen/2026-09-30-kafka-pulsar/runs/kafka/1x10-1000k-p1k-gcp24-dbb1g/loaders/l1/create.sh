#!/usr/bin/env bash
# run.sh 1x10-1000k-p1k-gcp24-dbb1g: create (generated 2026-10-14T12:09:48Z); pid in create.pid, exit code in create.rc
cd /root/bench/runs/kafka/1x10-1000k-p1k-gcp24-dbb1g || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x10-1000k-p1k-gcp24-dbb1g -partitions 10 -rf 3 -min-isr 2 -create-only -topic-timeout 1800s -tag 1x10-1000k-p1k-gcp24-dbb1g-create -out /root/bench/runs/kafka/1x10-1000k-p1k-gcp24-dbb1g/create.json  > create.log 2>&1 < /dev/null &
echo $! > create.pid.tmp && mv create.pid.tmp create.pid
wait $!
echo $? > create.rc.tmp && mv create.rc.tmp create.rc
