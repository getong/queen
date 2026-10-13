#!/usr/bin/env bash
# lateprobe.sh <load start, unix ms> <out dir> : on the leader, the apply thread before and after the 70th second of a
# load (2026-10-12: from about there the end to end p99 rises and the apply stage shows stalls of tens of ms, with
# nothing deleted and dedup off). Two windows, seconds 36-54 (A) and 72-89 (B): where the thread waits (offcpu.bt),
# what it runs (perf, on the CPU), and the kernel's memory counters around each. The perf reports are made at the end,
# after the load, so that they take no CPU from it.
START=$1; OUT=$2; mkdir -p "$OUT"; PID=$(pgrep -x queen | head -1)
AT=$(grep -l -x queen-rsm-apply /proc/$PID/task/*/comm 2>/dev/null | head -1 | cut -d/ -f5)
echo "pid $PID apply tid $AT" > "$OUT/ids"
snap() { cat /proc/vmstat > "$OUT/vmstat-$1"; cat /proc/$PID/smaps_rollup > "$OUT/smaps-$1" 2>/dev/null; cat /proc/$PID/task/$AT/schedstat > "$OUT/schedstat-$1"; grep ctxt /proc/$PID/task/$AT/status > "$OUT/ctxt-$1"; head -1 /proc/stat > "$OUT/cpu-$1"; }
win() { # <name> <from second> <seconds>
  local w=$(( START / 1000 + $2 - $(date +%s) )); [ $w -gt 0 ] && sleep $w
  snap $1-a
  bpftrace /root/p3/offcpu.bt $AT $PID > "$OUT/offcpu-$1.txt" 2>&1 &
  local B=$!
  perf record -q -o "$OUT/perf-$1.data" -F 499 -e cpu-clock --call-graph dwarf,16384 -t $AT -- sleep $(( $3 - 3 )) > "$OUT/perf-$1.log" 2>&1
  kill -INT $B; wait $B 2>/dev/null
  snap $1-b
}
win A 36 18
win B 72 17
sleep 22
for w in A B; do perf report -i "$OUT/perf-$w.data" --no-children --percent-limit 1.2 --stdio 2>/dev/null | grep -v '^$' | head -220 > "$OUT/perf-$w.txt"; done
rm -f "$OUT"/perf-*.data
echo PROBE_DONE > "$OUT/done"
