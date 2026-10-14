#!/usr/bin/env bash
# run.sh 1x100000-2000k-oct13: create (generated 2026-10-13T10:26:41Z); pid in create.pid, exit code in create.rc
cd /root/bench/runs/kafka/1x100000-2000k-oct13 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic k1x100000-2000k-oct13 -partitions 100000 -rf 3 -min-isr 2 -create-only -warm -topic-timeout 1800s -topic-settle 30s -tag 1x100000-2000k-oct13-create -out /root/bench/runs/kafka/1x100000-2000k-oct13/create.json  > create.log 2>&1 < /dev/null &
echo $! > create.pid.tmp && mv create.pid.tmp create.pid
wait $!
echo $? > create.rc.tmp && mv create.rc.tmp create.rc
