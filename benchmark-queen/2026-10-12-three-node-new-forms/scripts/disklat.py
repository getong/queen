#!/usr/bin/env python3
"""disklat.py <step dir> [seconds per row]: the brokers' disks through a step, from the per-second sampler
(netdisk.sh, 2026-10-13 form): MB/s written, write requests a second and their mean size, the device's mean write
latency (ms a request, wticks / wios), how busy the device was (share of the time with a request in flight), the
mean number of requests in flight, flushes a second with their mean ms, discards a second. One row per interval and
node; t counts from the load's start."""
import glob, json, os, sys

d = sys.argv[1].rstrip("/")
step = int(sys.argv[2]) if len(sys.argv) > 2 else 30
t0 = json.load(open(sorted(glob.glob(os.path.join(d, "q*.json")))[0]))["start_ms"] / 1000
for n in (1, 2, 3):
    f = os.path.join(d, f"net-n{n}.log")
    if not os.path.exists(f):
        continue
    rs = [dict(x.split("=", 1) for x in l.split() if "=" in x) for l in open(f)]
    rs = [r for r in rs if "wios" in r]
    if len(rs) < 3:
        print(f"n{n}: the sampler of this step has no device latency counters"); continue
    print(f"n{n}:    t   MB/s  writes/s  KB each  latency ms  busy %  in flight  flushes/s  flush ms  discards/s")
    a = 0
    while True:
        w = [r for r in rs if t0 + a <= float(r["t"]) <= t0 + a + step]
        if len(w) < 2:
            if a > 0: break
            a += step; continue
        x, y = w[0], w[-1]
        dt = float(y["t"]) - float(x["t"]); g = lambda k: int(y[k]) - int(x[k])
        ios = max(1, g("wios")); fl = max(1, g("fios"))
        print(f"   {a:>4}  {g('wsec') * 512 / 1e6 / dt:5.0f}  {ios / dt:8.0f}  {g('wsec') * 512 / 1024 / ios:7.0f}  {g('wticks') / ios:10.3f}  {g('ioticks') / dt / 10:6.1f}  {g('tiq') / dt / 1000:9.2f}  "
              f"{g('fios') / dt:9.0f}  {g('fticks') / fl:8.3f}  {g('dios') / dt:10.0f}")
        a += step
