#!/usr/bin/env bash
# rehashprobe.sh <load start, unix ms> <out dir> : on the leader, every thread of the broker on the CPU with its call
# stack (perf) in two short windows of a load. 2026-10-13: with a million partitions or more, one 10 s window of a
# 2M msg/s point shows a push p99 of 100 ms while commit, flush and apply look as in every other window; it falls
# where the count of partitions written since the start passes 917,504 (7/8 of 2^20; 458,752 gives a smaller one):
# second 51 and second 28 at 2M msg/s. W1 / W2: where each window starts (seconds of the load), LEN: its length.
START=$1; OUT=$2; mkdir -p "$OUT"; PID=$(pgrep -x queen | head -1)
W1=${W1:-25.5}; W2=${W2:-48.5}; LEN=${LEN:-5.5}
python3 -c "import time; print(time.time() - time.clock_gettime(time.CLOCK_MONOTONIC))" > "$OUT/wall-minus-mono"
win() { # <name> <from second> <seconds>
  sleep "$(python3 -c "import time; print(max(0.0, $START / 1000 + $2 - time.time()))")"
  perf record -q -k CLOCK_MONOTONIC -o /dev/shm/rp-$1.data -F 499 -e cpu-clock --call-graph dwarf,8192 -p "$PID" -- sleep "$3" > "$OUT/perf-$1.log" 2>&1
}
win A "$W1" "$LEN"
win B "$W2" "$LEN"
sleep "$(python3 -c "import time; print(max(0.0, $START / 1000 + ${AFTER:-96} - time.time()))")"
for n in A B; do perf script -i /dev/shm/rp-$n.data -F comm,tid,time,ip,sym --no-inline 2>>"$OUT/perf-$n.log" | gzip > "$OUT/stacks-$n.txt.gz"; done
ls -la /dev/shm/rp-*.data >> "$OUT/perf-A.log" 2>&1; rm -f /dev/shm/rp-*.data
echo PROBE_DONE > "$OUT/done"
