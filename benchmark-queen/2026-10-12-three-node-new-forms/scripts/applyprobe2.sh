#!/usr/bin/env bash
# applyprobe2.sh <load start, unix ms> <out dir> : on the leader, a long window of a load (W: its first second,
# default 100; LEN: its length, default 95 s) on the apply threads alone: what they run (perf, with stacks) and
# where they wait, with every wait of 15 ms or more listed (offcpu3.bt). For stalls that come once a minute.
START=$1; OUT=$2; mkdir -p "$OUT"; PID=$(pgrep -x queen | head -1)
W=${W:-100}; LEN=${LEN:-95}
for t in /proc/$PID/task/*; do echo "$(basename $t) $(cat $t/comm)"; done > "$OUT/threads"
TIDS=$(awk '$2 == "queen-rsm-apply" {print $1}' "$OUT/threads" | paste -sd, -)
python3 -c "import time; print(time.time() - time.clock_gettime(time.CLOCK_MONOTONIC))" > "$OUT/wall-minus-mono"
sleep "$(python3 -c "import time; print(max(0.0, $START / 1000 + $W - time.time()))")"
bpftrace /root/p3/offcpu3.bt > "$OUT/offcpu.txt" 2>&1 &
B=$!
sleep 1.5
perf record -q -k CLOCK_MONOTONIC -o /dev/shm/ap.data -F 499 -e cpu-clock --call-graph dwarf,8192 -t "$TIDS" -- sleep "$LEN" > "$OUT/perf.log" 2>&1
kill -INT $B; wait $B 2>/dev/null
sleep "$(python3 -c "import time; print(max(0.0, $START / 1000 + ${AFTER:-226} - time.time()))")"
perf script -i /dev/shm/ap.data -F comm,tid,time,ip,sym --no-inline 2>>"$OUT/perf.log" | gzip > "$OUT/stacks.txt.gz"
ls -la /dev/shm/ap.data >> "$OUT/perf.log" 2>&1; rm -f /dev/shm/ap.data
echo PROBE_DONE > "$OUT/done"
