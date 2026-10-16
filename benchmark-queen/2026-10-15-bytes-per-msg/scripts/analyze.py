#!/usr/bin/env python3
"""Bytes per message from a bpm sample.jsonl, over windows of seconds after the load started.

usage: analyze.py <sample.jsonl> <window> [<window> ...]   window = a-b (seconds after the first second with pushes)
       analyze.py <sample.jsonl> all <messages>             the whole file, messages given (old samples)
"""
import json
import sys


def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    return rows


def pushed(r):
    return r.get("m", {}).get("queen_process_push_messages_total", 0.0)


def thr(r, name, field):
    return r["thr"].get(name, {}).get(field, 0)


def fsize(r, kind, field="size"):
    return sum(v[field] for k, v in r["files"].items() if k == kind or k.startswith(kind))


def report(a, b, msgs, label):
    dt = b["t"] - a["t"]
    rate = msgs / dt
    print(f"\n== {label}: {dt:.0f} s, {msgs:,.0f} messages, {rate:,.0f} msg/s")

    def line(name, bytes_):
        print(f"  {name:<44} {bytes_/dt/1e6:9.2f} MB/s {bytes_/msgs:9.2f} B/msg")

    blk = (b["blk"]["wsec"] - a["blk"]["wsec"]) * 512
    line("block device (ext4 loop): everything", blk)
    line("process write_bytes (pages dirtied)", b["proc"]["write_bytes"] - a["proc"]["write_bytes"])
    line("process wchar (write syscalls, incl. net)", b["proc"]["wchar"] - a["proc"]["wchar"])
    names = sorted(set(a["thr"]) | set(b["thr"]))
    rows = []
    for n in names:
        dw = thr(b, n, "wchar") - thr(a, n, "wchar")
        db = thr(b, n, "wb") - thr(a, n, "wb")
        if dw > 0 or db > 0:
            rows.append((db, dw, n))
    rows.sort(reverse=True)
    print("  per thread name:                                write_bytes        wchar")
    for db, dw, n in rows[:12]:
        print(f"    {n:<20} {db/dt/1e6:9.2f} MB/s {db/msgs:8.2f} B/msg | {dw/dt/1e6:9.2f} MB/s {dw/msgs:8.2f} B/msg")
    print("  files (growth of their sizes):")
    kinds = sorted(set(b["files"]) | set(a["files"]))
    for k in kinds:
        d = b["files"].get(k, {}).get("size", 0) - a["files"].get(k, {}).get("size", 0)
        al = b["files"].get(k, {}).get("alloc", 0) - a["files"].get(k, {}).get("alloc", 0)
        if d or al:
            print(f"    {k:<30} size {d/msgs:9.2f} B/msg  alloc {al/msgs:9.2f} B/msg  (now {b['files'][k]['size']/1e6:,.1f} MB)" if k in b["files"] else f"    {k}: gone")
    m0, m1 = a.get("m", {}), b.get("m", {})
    for k in sorted(m1):
        if k != "queen_process_push_messages_total":
            print(f"    metric {k}: {(m1[k]-m0.get(k,0))/dt:,.1f}/s")


def main():
    rows = load(sys.argv[1])
    if sys.argv[2] == "all":
        report(rows[0], rows[-1], float(sys.argv[3]), "whole file")
        return
    # t0: the first second whose push counter moved by more than 1000
    t0 = None
    for i in range(1, len(rows)):
        if pushed(rows[i]) - pushed(rows[i - 1]) > 1000:
            t0 = rows[i - 1]["t"]
            break
    if t0 is None:
        print("no load found")
        return
    for w in sys.argv[2:]:
        s, e = (float(x) for x in w.split("-"))
        sel = [r for r in rows if s <= r["t"] - t0 <= e]
        if len(sel) < 2:
            print(f"window {w}: no samples")
            continue
        a, b = sel[0], sel[-1]
        msgs = pushed(b) - pushed(a)
        report(a, b, msgs, f"window {w} s")


if __name__ == "__main__":
    main()
