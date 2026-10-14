#!/usr/bin/env python3
"""Broker and host counters at one instant, and the difference of two.

  snap.py take <out.json>              every `queen` process on this host
  snap.py diff <before.json> <after.json>

`take` reads, per broker process: CPU ticks per thread (grouped by thread
name), its /metrics/prometheus, its RSS; and for the host: /proc/stat and the
disk's write and flush counters. `diff` prints rates over the interval: cores
per thread group, events per second and mean per event of every histogram.
"""
import json
import os
import re
import sys
import time
import urllib.request

TICK = os.sysconf("SC_CLK_TCK")


def threads(pid):
    out = {}
    base = f"/proc/{pid}/task"
    for tid in os.listdir(base):
        try:
            with open(f"{base}/{tid}/stat") as f:
                s = f.read()
        except OSError:
            continue
        r = s.rindex(")")
        comm = s[s.index("(") + 1:r]
        rest = s[r + 2:].split()
        key = re.sub(r"[-_ ]?\d+$", "", comm)
        out[key] = out.get(key, 0) + int(rest[11]) + int(rest[12])
    return out


def env_of(pid):
    try:
        with open(f"/proc/{pid}/environ", "rb") as f:
            raw = f.read().split(b"\0")
    except OSError:
        return {}
    return dict(kv.decode(errors="replace").split("=", 1) for kv in raw if b"=" in kv)


def metrics(port):
    out = {}
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics/prometheus", timeout=5) as r:
            text = r.read().decode()
    except Exception as e:  # a snapshot never fails a run
        return {"_error": str(e)}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        name, _, value = line.rpartition(" ")
        if not name.startswith(("queen_raft_", "queen_kv_", "queen_locks_")):
            continue
        try:
            out[name] = float(value)
        except ValueError:
            pass
    return out


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return 0


def host():
    with open("/proc/stat") as f:
        cpu = [int(x) for x in f.readline().split()[1:9]]
    disk = {}
    with open("/proc/diskstats") as f:
        for line in f:
            p = line.split()
            if p[2] == "vda":
                disk = {"writes": int(p[7]), "sectors": int(p[9]), "write_ms": int(p[10]),
                        "flushes": int(p[18]) if len(p) > 18 else 0,
                        "flush_ms": int(p[19]) if len(p) > 19 else 0}
    return {"cpu": cpu, "disk": disk}


def take(out):
    snap = {"t": time.time(), "host": host(), "nodes": {}}
    for pid in sorted(int(p) for p in os.listdir("/proc") if p.isdigit()):
        try:
            with open(f"/proc/{pid}/comm") as f:
                if f.read().strip() != "queen":
                    continue
        except OSError:
            continue
        port = env_of(pid).get("PORT", "6632")
        snap["nodes"][port] = {"pid": pid, "threads": threads(pid), "rss_kb": rss_kb(pid),
                               "metrics": metrics(port)}
    with open(out, "w") as f:
        json.dump(snap, f)


def diff(a, b):
    with open(a) as f:
        a = json.load(f)
    with open(b) as f:
        b = json.load(f)
    secs = b["t"] - a["t"]
    print(f"interval {secs:.1f} s")
    ca, cb = a["host"]["cpu"], b["host"]["cpu"]
    d = [(y - x) / TICK / secs for x, y in zip(ca, cb)]
    busy = d[0] + d[1] + d[2] + d[5] + d[6]
    print(f"host: busy {busy:.2f} cores (user {d[0]:.2f} sys {d[2]:.2f} softirq {d[6]:.2f}) "
          f"iowait {d[4]:.2f} steal {d[7]:.2f} idle {d[3]:.2f}")
    da, db = a["host"]["disk"], b["host"]["disk"]
    if da and db:
        w = db["writes"] - da["writes"]
        fl = db["flushes"] - da["flushes"]
        print(f"disk vda: {w / secs:.0f} writes/s, {(db['sectors'] - da['sectors']) * 512 / 1e6 / secs:.1f} MB/s, "
              f"{fl / secs:.0f} flushes/s"
              + (f", {(db['flush_ms'] - da['flush_ms']) / fl:.2f} ms per flush" if fl else ""))
    for port in sorted(b["nodes"]):
        nb, na = b["nodes"][port], a["nodes"].get(port)
        if not na or na["pid"] != nb["pid"]:
            print(f"node :{port}: restarted inside the interval")
            continue
        th = {k: (v - na["threads"].get(k, 0)) / TICK / secs for k, v in nb["threads"].items()}
        total = sum(th.values())
        print(f"node :{port} pid {nb['pid']}: {total:.2f} cores, rss {nb['rss_kb'] / 1024:.0f} MiB")
        top = sorted(th.items(), key=lambda kv: -kv[1])
        print("  threads: " + "  ".join(f"{k} {v:.2f}" for k, v in top if v >= 0.005))
        ma, mb = na["metrics"], nb["metrics"]
        rows = []
        for name in sorted(mb):
            if not name.endswith("_count") and "_count{" not in name:
                continue
            base = name.replace("_count", "", 1)
            sum_name = name.replace("_count", "_sum", 1)
            dc = mb[name] - ma.get(name, 0)
            ds = mb.get(sum_name, 0) - ma.get(sum_name, 0)
            if dc <= 0:
                continue
            rows.append((base, dc / secs, ds / dc, ds / secs))
        for base, per_s, mean, total_s in rows:
            if base.endswith("_seconds") or "_seconds{" in base:
                print(f"  {base}: {per_s:.0f}/s, mean {mean * 1e6:.0f} us, {total_s:.3f} s per s")
            elif "milliseconds" in base:
                print(f"  {base}: {per_s:.0f}/s, mean {mean:.3f} ms")
            else:
                print(f"  {base}: {per_s:.0f}/s, mean {mean:.1f}")
        for name in sorted(mb):
            if name.endswith("_total") or "_total{" in name:
                dv = mb[name] - ma.get(name, 0)
                if dv > 0:
                    print(f"  {name}: {dv / secs:.0f}/s")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "take":
        take(sys.argv[2])
    elif len(sys.argv) == 4 and sys.argv[1] == "diff":
        diff(sys.argv[2], sys.argv[3])
    else:
        sys.exit(__doc__)
