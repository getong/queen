#!/bin/bash
# ceilprobe.sh <tag> [cores, default 5] [seconds, default 15] : on a broker. Waits until the broker has been above
# that many cores for that long (a load step past its ramp; a higher figure picks a later step of a ramp),
# then for about 16 s takes: every file sync by thread (synctrace.bt), CPU per thread, the device (iostat), the ext4
# journal's counters before and after, and a flat CPU profile of the whole process (perf, 199 Hz, no call graphs);
# then 5 s of call graphs on the apply and raft threads.
TAG=$1; export HOTC=${2:-5} HOTS=${3:-15}; OUT=/root/prof/$TAG; mkdir -p "$OUT"; cd "$OUT" || exit 1
PID=$(python3 - <<'PY'
import os, subprocess, time
HOTC, HOTS = float(os.environ['HOTC']), int(os.environ['HOTS'])
def pid():
    try: return int(subprocess.check_output(["pgrep", "-n", "-x", "queen"]).split()[0])
    except Exception: return None
def ticks(p):
    try:
        s = open(f"/proc/{p}/stat").read(); f = s[s.rindex(")") + 2:].split(); return int(f[11]) + int(f[12])
    except Exception: return None
hot, last, lp = 0, None, None
while True:
    p = pid(); t = ticks(p) if p else None
    if p and lp == p and last is not None and t is not None:
        hot = hot + 1 if (t - last) / 100.0 > HOTC else 0
        if hot >= HOTS: break
    lp, last = p, t
    time.sleep(1)
print(p)
PY
)
date -u +%T.%N > started.txt
ls -l /proc/$PID/fd > fds.txt 2>/dev/null
J=$(ls -d /proc/fs/jbd2/*/ | head -1); cat "$J/info" > jbd2-a.txt; date +%s.%N >> jbd2-a.txt
(iostat -dxm 1 15 > iostat.txt 2>&1 &)
(timeout -s INT 14 bpftrace /root/p3/synctrace.bt "$PID" > sync.txt 2>&1 &)
(python3 /root/p3/pthreads.py "$PID" 12 > threads.txt 2>&1 &)
perf record -o perf.data -F 199 -e cpu-clock -p "$PID" -- sleep 10 > perf.log 2>&1
sleep 5
cat "$J/info" > jbd2-b.txt; date +%s.%N >> jbd2-b.txt
T=$(ps -L -o tid=,comm= -p "$PID" | awk '$2 == "queen-rsm-apply" || $2 == "queen-raft" {print $1}' | paste -sd,)
perf record -o perf-cg.data -F 499 -e cpu-clock --call-graph dwarf,16384 -t "$T" -- sleep 5 > perf-cg.log 2>&1
curl -s -m 3 "http://$(grep -o 'PRIV=.*' /root/p3/node.env | cut -d= -f2):6632/metrics/prometheus" | grep -v '^#' | grep -E 'queen_raft_(log_fsync|durable_point|checkpoint_write|apply|commit|propose|pipeline|inflight|admit)' | grep -v _bucket > metrics.txt 2>/dev/null
perf report -i perf.data --no-children --sort comm,symbol --stdio 2>/dev/null | grep -v -E '^#|^$' | head -90 > perf-top.txt
echo PROBE_DONE > done.txt
