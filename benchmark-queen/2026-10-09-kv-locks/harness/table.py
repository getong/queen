#!/usr/bin/env python3
"""table.py <runs dir> [topology] — one line per measured point, from the
loader's [json] summary and the broker counters of the same window."""
import json
import os
import re
import sys


def load(run):
    out = {}
    try:
        with open(os.path.join(run, "load.txt")) as f:
            for line in f:
                if line.startswith("[json] "):
                    out = json.loads(line[7:])
    except OSError:
        return None
    if not out:
        return None
    b = {}
    try:
        with open(os.path.join(run, "broker.txt")) as f:
            text = f.read()
    except OSError:
        text = ""
    m = re.search(r"host: busy ([\d.]+) cores.*idle ([\d.]+)", text)
    if m:
        b["hostBusy"], b["hostIdle"] = float(m.group(1)), float(m.group(2))
    nodes = re.findall(r"node :(\d+) pid \d+: (-?[\d.]+) cores, rss (\d+) MiB\n  threads: (.*)", text)
    # The brokers' CPU is the machine's busy cores minus the loader's: nothing
    # else runs there. The sum over a broker's threads is not used, because a
    # thread that exits inside the window takes its ticks with it (a kept
    # broker reaps idle workers after a point with more connections).
    b["brokerCores"] = max(b.get("hostBusy", 0.0) - out.get("loaderCores", 0.0), 0.0)
    b["rssMiB"] = max((int(n[2]) for n in nodes), default=0)
    planner = 0.0
    for n in nodes:
        pm = re.search(r"queen-planner ([\d.]+)", n[3])
        if pm:
            planner = max(planner, float(pm.group(1)))
    b["planner"] = planner
    m = re.search(r"queen_raft_log_fsync_seconds: (\d+)/s, mean (\d+) us", text)
    if m:
        b["fsyncPerS"], b["fsyncUs"] = int(m.group(1)), int(m.group(2))
    m = re.search(r"queen_raft_drain_commands: (\d+)/s, mean ([\d.]+)", text)
    if m:
        b["cyclesPerS"], b["cmdsPerCycle"] = int(m.group(1)), float(m.group(2))
    m = re.search(r"queen_raft_proposals_total: (\d+)/s", text)
    if m:
        b["proposals"] = int(m.group(1))
    # The busy share of the three serial stages of a write, seconds per second
    # (the largest node's, on a cluster: the leader's).
    for key, name in (("loopBusy", "drain_to_propose"), ("applyBusy", "apply_entry"), ("logBusy", "log_fsync"),
                      ("ckptBusy", "checkpoint_write")):
        vals = [float(v) for v in re.findall(rf"queen_raft_{name}_seconds: \d+/s, mean \d+ us, ([\d.]+) s per s", text)]
        b[key] = max(vals, default=0.0)
    m = re.search(r"disk vda: (\d+) writes/s, ([\d.]+) MB/s", text)
    if m:
        b["diskMB"] = float(m.group(2))
    out["broker"] = b
    return out


def main():
    root = sys.argv[1]
    topos = sys.argv[2:] or sorted(d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d)))
    for topo in topos:
        d = os.path.join(root, topo)
        runs = sorted(os.listdir(d), key=lambda t: os.path.getmtime(os.path.join(d, t)))
        print(f"\n## {topo}\n")
        print("| point | workers | ops/s | calls/s | p50 ms | p99 ms | p99.9 ms | errors | broker cores | planner | "
              "host idle | loader cores | cmds/cycle | fsync/s |")
        print("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
        for tag in runs:
            r = load(os.path.join(d, tag))
            if not r:
                continue
            head = "call" if "call" in r["series"] else ("cycle" if "cycle" in r["series"] else "try")
            s = r["series"][head]
            b = r["broker"]
            print(f"| {tag} | {r['workers']} | {r['opsPerSec']:,.0f} | {s['perSec']:,.0f} | {s['p50']:.2f} | "
                  f"{s['p99']:.2f} | {s['p999']:.1f} | {r['errTotal']} | {b.get('brokerCores', 0):.1f} | "
                  f"{b.get('planner', 0):.2f} | {b.get('hostIdle', 0):.1f} | {r['loaderCores']:.1f} | "
                  f"{b.get('cmdsPerCycle', 0):.1f} | {b.get('fsyncPerS', 0):,} |")


if __name__ == "__main__":
    main()
