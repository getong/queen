#!/usr/bin/env bash
# run.sh 1x1000-3000k-gcp24: create (generated 2026-10-16T07:18:28Z); pid in create.pid, exit code in create.rc
cd /root/bench/runs/redpanda/1x1000-3000k-gcp24 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400 GOMEMLIMIT=9GiB
/root/bench/bin/kload -brokers 10.250.0.23:9092,10.250.0.22:9092,10.250.0.25:9092 -topics 1 -topic r1x1000-3000k-gcp24 -partitions 1000 -rf 3 -min-isr 0 -group-protocol classic -create-only -topic-timeout 1800s -tag 1x1000-3000k-gcp24-create -out /root/bench/runs/redpanda/1x1000-3000k-gcp24/create.json  > create.log 2>&1 < /dev/null &
echo $! > create.pid.tmp && mv create.pid.tmp create.pid
wait $!
echo $? > create.rc.tmp && mv create.rc.tmp create.rc
