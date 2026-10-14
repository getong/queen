#!/usr/bin/env python3
"""tsched.py <pid> <seconds> [interface] : every thread of the broker over an interval, from /proc.

For each thread: the CPU it used, the time it spent runnable and waiting for a CPU (the second number of
/proc/<pid>/task/<tid>/schedstat), how many times it was put on a CPU, how many times it was moved to
another CPU and how many times it was taken off one against its will. Grouped by thread name, then the
busiest threads one by one. A thread name in /proc is cut at 15 characters, so the workers of a pool carry
one name. With an interface, its bytes and the TCP retransmits over the same interval.
"""
import os, sys, time

pid = sys.argv[1]; secs = float(sys.argv[2]); iface = sys.argv[3] if len(sys.argv) > 3 else None
base = f"/proc/{pid}/task"


def threads():
    out = {}
    for tid in os.listdir(base):
        try:
            comm = open(f"{base}/{tid}/comm").read().strip()
            run, wait, runs = (int(x) for x in open(f"{base}/{tid}/schedstat").read().split())
            mig = invol = 0
            for line in open(f"{base}/{tid}/sched"):
                if line.startswith("se.nr_migrations"):
                    mig = int(line.split(":")[1])
                elif line.startswith("nr_involuntary_switches"):
                    invol = int(line.split(":")[1]); break
            out[tid] = (comm, run, wait, runs, mig, invol)
        except Exception:
            pass
    return out


def net():
    tx = rx = 0
    if iface:
        for line in open("/proc/net/dev"):
            if line.strip().startswith(iface + ":"):
                f = line.split(":")[1].split(); rx, tx = int(f[0]), int(f[8])
    seg = retr = 0
    rows = [l.split() for l in open("/proc/net/snmp") if l.startswith("Tcp:")]
    if len(rows) == 2:
        d = dict(zip(rows[0][1:], rows[1][1:])); seg, retr = int(d["OutSegs"]), int(d["RetransSegs"])
    return rx, tx, seg, retr


t0 = time.time(); a = threads(); na = net()
time.sleep(secs)
t1 = time.time(); b = threads(); nb = net()
dt = t1 - t0
rows = []
for tid, y in b.items():
    x = a.get(tid)
    if x is None:
        continue
    rows.append((y[0], tid, (y[1] - x[1]) / dt / 1e7, (y[2] - x[2]) / dt / 1e6, (y[3] - x[3]) / dt, (y[4] - x[4]) / dt, (y[5] - x[5]) / dt))
# name, tid, cpu % of a core, wait ms per second, runs per second, migrations per second, involuntary switches per second
print(time.strftime("%H:%M:%SZ", time.gmtime(t0)), f"over {dt:.1f} s: {len(rows)} threads, {sum(r[2] for r in rows) / 100:.2f} cores used,",
      f"{sum(r[3] for r in rows):.1f} ms of every second spent runnable and waiting for a CPU, all threads together")
if iface:
    print(f"{iface}: sent {(nb[1] - na[1]) / dt / 1e6:.1f} MB/s, received {(nb[0] - na[0]) / dt / 1e6:.1f} MB/s;",
          f"TCP segments out {(nb[2] - na[2]) / dt:,.0f}/s, retransmitted {(nb[3] - na[3]) / dt:,.1f}/s")
g = {}
for r in rows:
    g.setdefault(r[0], []).append(r)
print("by name            threads   cpu %  busiest %  wait ms/s  worst ms/s  wait per run us   runs/s   moved/s  preempted/s")
for name, rs in sorted(g.items(), key=lambda kv: -sum(r[2] for r in kv[1])):
    cpu = sum(r[2] for r in rs)
    if cpu < 0.5 and sum(r[3] for r in rs) < 0.5:
        continue
    runs = sum(r[4] for r in rs); wait = sum(r[3] for r in rs)
    print(f"  {name:16} {len(rs):7d} {cpu:7.1f} {max(r[2] for r in rs):10.1f} {wait:10.2f} {max(r[3] for r in rs):11.2f} {(1000 * wait / runs if runs else 0):16.1f} {runs:8.0f} {sum(r[5] for r in rs):9.1f} {sum(r[6] for r in rs):12.1f}")
print("the 45 busiest threads   tid      cpu %  wait ms/s  wait per run us   runs/s   moved/s  preempted/s")
for r in sorted(rows, key=lambda r: -r[2])[:45]:
    print(f"  {r[0]:16} {r[1]:>9} {r[2]:8.1f} {r[3]:10.2f} {(1000 * r[3] / r[4] if r[4] else 0):16.1f} {r[4]:8.0f} {r[5]:9.1f} {r[6]:12.1f}")
