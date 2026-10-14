#!/usr/bin/env bash
# run.sh 1x100000-2000k-oct13: p1 (generated 2026-10-13T10:32:14Z); pid in p1.pid, exit code in p1.rc
cd /root/bench/runs/kafka/1x100000-2000k-oct13 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic k1x100000-2000k-oct13 -partitions 100000 -entities 0 -create=false -rate 222222 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x100000-2000k-oct13/p1.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 33 -cons-total 297 -poll 1000 -ack async -loader-index 1 -loaders 9 -local-src 0-2 -out /root/bench/runs/kafka/1x100000-2000k-oct13/p1.json -tag 1x100000-2000k-oct13-p1 -group-timeout 600s  > p1.log 2>&1 < /dev/null &
echo $! > p1.pid.tmp && mv p1.pid.tmp p1.pid
wait $!
echo $? > p1.rc.tmp && mv p1.rc.tmp p1.rc
