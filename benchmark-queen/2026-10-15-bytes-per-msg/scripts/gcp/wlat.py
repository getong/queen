#!/usr/bin/env python3
"""ms a write on each broker's md0 over lines 30-60 of net-n*.log (wticks / wios deltas, as /sys/block/md0/stat
counts them), and the busy share (ioticks). usage: wlat.py <step dir> [...]"""
import os
import sys


def rows(f):
    out = []
    for l in open(f):
        d = dict(p.split("=", 1) for p in l.split() if "=" in p)
        if "wios" in d:
            out.append({k: float(v) for k, v in d.items()})
    return out


for step in sys.argv[1:]:
    cells = []
    for n in (1, 2, 3):
        f = os.path.join(step, f"net-n{n}.log")
        r = rows(f) if os.path.exists(f) else []
        if len(r) < 61:
            cells.append(f"n{n} ?")
            continue
        a, b = r[30], r[60]
        ios = b["wios"] - a["wios"]
        ms = (b["wticks"] - a["wticks"]) / ios if ios else float("nan")
        busy = (b["ioticks"] - a["ioticks"]) / ((b["t"] - a["t"]) * 1000) * 100
        cells.append(f"n{n} {ms:.3f} ms a write, busy {busy:.0f}%")
    print(f"{step.rstrip('/').split('/')[-2]}: " + " | ".join(cells))
