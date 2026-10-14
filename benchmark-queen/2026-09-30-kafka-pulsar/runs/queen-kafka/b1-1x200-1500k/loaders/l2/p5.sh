#!/usr/bin/env bash
# run.sh b1-1x200-1500k: p5 (generated 2026-10-13T11:03:31Z); pid in p5.pid, exit code in p5.rc
cd /root/bench/runs/queen-kafka/b1-1x200-1500k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload -brokers 10.114.0.4:9092,10.114.0.5:9092,10.114.0.6:9092 -topics 1 -topic kb1-1x200-1500k -partitions 200 -entities 0 -create=false -rate 166666 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/queen-kafka/b1-1x200-1500k/p5.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 22 -cons-offset 110 -cons-total 198 -poll 1000 -ack async -loader-index 5 -loaders 9 -local-src 3-5 -out /root/bench/runs/queen-kafka/b1-1x200-1500k/p5.json -tag b1-1x200-1500k-p5 -group-protocol classic  > p5.log 2>&1 < /dev/null &
echo $! > p5.pid.tmp && mv p5.pid.tmp p5.pid
wait $!
echo $? > p5.rc.tmp && mv p5.rc.tmp p5.rc
