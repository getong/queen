#!/usr/bin/env bash
# run.sh b1-1x200-1500k: create (generated 2026-10-13T11:03:19Z); pid in create.pid, exit code in create.rc
cd /root/bench/runs/queen-kafka/b1-1x200-1500k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x200-1500k -partitions 200 -rf 3 -min-isr 1 -create-only -topic-config retention.ms=600000 -topic-timeout 1800s -tag b1-1x200-1500k-create -out /root/bench/runs/queen-kafka/b1-1x200-1500k/create.json  > create.log 2>&1 < /dev/null &
echo $! > create.pid.tmp && mv create.pid.tmp create.pid
wait $!
echo $? > create.rc.tmp && mv create.rc.tmp create.rc
