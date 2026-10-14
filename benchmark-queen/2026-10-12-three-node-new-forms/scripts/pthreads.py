#!/usr/bin/env python3
"""threads.py <pid> <seconds>: CPU per thread name of a process over a window."""
import collections, os, sys, time

pid, secs = sys.argv[1], float(sys.argv[2])
hz = os.sysconf("SC_CLK_TCK")


def snap():
    out = {}
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            s = open(f"/proc/{pid}/task/{tid}/stat").read()
            comm = s[s.index("(") + 1 : s.rindex(")")]
            f = s[s.rindex(")") + 2 :].split()
            out[tid] = (comm, int(f[11]), int(f[12]))  # utime, stime
        except Exception:
            pass
    return out


a = snap()
t0 = time.time()
time.sleep(secs)
b = snap()
dt = time.time() - t0
g = collections.defaultdict(lambda: [0, 0, 0])
for tid, (comm, u, s) in b.items():
    if tid in a:
        g[comm][0] += u - a[tid][1]
        g[comm][1] += s - a[tid][2]
        g[comm][2] += 1
rows = sorted(g.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))
tot = sum(v[0] + v[1] for _, v in rows) / hz / dt
print(f"total {tot:.2f} cores over {dt:.1f}s")
for comm, (u, s, n) in rows:
    c = (u + s) / hz / dt
    if c >= 0.005:
        print(f"{comm:<18} threads {n:>3}  cores {c:5.2f}  user {u / hz / dt:5.2f}  sys {s / hz / dt:5.2f}")
