#!/usr/bin/env bash
# applyprobe.sh <load start, unix ms> <out dir> : on the leader, one window of a load (W: its first second, default
# 150; LEN: its length, default 8 s): every thread of the broker on the CPU with its call stack (perf), and where the
# apply threads wait (offcpu2.bt). 2026-10-13: with retention on and a million partitions, apply's wait is what the
# end to end p99 follows, and no thread name is out of CPU.
START=$1; OUT=$2; mkdir -p "$OUT"; PID=$(pgrep -x queen | head -1)
W=${W:-150}; LEN=${LEN:-8}
for t in /proc/$PID/task/*; do echo "$(basename $t) $(cat $t/comm)"; done > "$OUT/threads"
sleep "$(python3 -c "import time; print(max(0.0, $START / 1000 + $W - time.time()))")"
bpftrace /root/p3/offcpu2.bt > "$OUT/offcpu.txt" 2>&1 &
B=$!
sleep 1.5
perf record -q -k CLOCK_MONOTONIC -o /dev/shm/ap.data -F 499 -e cpu-clock --call-graph dwarf,8192 -p "$PID" -- sleep "$LEN" > "$OUT/perf.log" 2>&1
kill -INT $B; wait $B 2>/dev/null
sleep "$(python3 -c "import time; print(max(0.0, $START / 1000 + ${AFTER:-226} - time.time()))")"
perf script -i /dev/shm/ap.data -F comm,tid,time,ip,sym --no-inline 2>>"$OUT/perf.log" | gzip > "$OUT/stacks.txt.gz"
rm -f /dev/shm/ap.data
echo PROBE_DONE > "$OUT/done"
