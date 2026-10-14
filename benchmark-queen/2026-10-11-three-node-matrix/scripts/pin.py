#!/usr/bin/env python3
"""pin.py <pid> <show | once | watch> : give the threads of the commit path their own cores, by thread name.

    the raft runtime (openraft's core, the replication streams, the RPC server)   PIN_RAFT   default 0-2
    the raft log writer and its fsync thread                                      PIN_LOG    default 3
    the main apply thread                                                         PIN_APPLY  default 4
    every other thread                                                            PIN_REST   default 5-15

A thread name in /proc is cut at 15 characters, so the main apply thread and its shard workers
(queen-rsm-apply-1 and on) carry one name: the main one is the one with the most CPU time at the first pass.
A thread made later takes the cores of the thread that made it, which may be the wrong ones (the compressors
start with the first sealed file), so `watch` goes over the threads again every second until the broker ends.
"""
import os, sys, time

pid = sys.argv[1]; mode = sys.argv[2] if len(sys.argv) > 2 else "show"
base = f"/proc/{pid}/task"


def cpus(spec):
    out = set()
    for part in spec.split(","):
        a, _, b = part.partition("-")
        out.update(range(int(a), int(b or a) + 1))
    return out


RAFT, LOG, APPLY, REST = (cpus(os.environ.get(k, d)) for k, d in (("PIN_RAFT", "0-2"), ("PIN_LOG", "3"), ("PIN_APPLY", "4"), ("PIN_REST", "5-15")))


def names():
    out = {}
    for tid in os.listdir(base):
        try:
            out[int(tid)] = open(f"{base}/{tid}/comm").read().strip()
        except Exception:
            pass
    return out


def fmt(s):
    s = sorted(s); out = []; i = 0
    while i < len(s):
        j = i
        while j + 1 < len(s) and s[j + 1] == s[j] + 1:
            j += 1
        out.append(str(s[i]) if i == j else f"{s[i]}-{s[j]}"); i = j + 1
    return ",".join(out)


if mode == "show":
    for tid, comm in sorted(names().items()):
        try:
            print(f"{comm} on {fmt(os.sched_getaffinity(tid))}")
        except Exception:
            pass
    sys.exit(0)

main = None


def one_pass():
    global main
    n = names()
    if main is None or main not in n:
        best = -1
        for tid, comm in n.items():
            if comm == "queen-rsm-apply":
                try:
                    run = int(open(f"{base}/{tid}/schedstat").read().split()[0])
                except Exception:
                    continue
                if run > best:
                    best, main = run, tid
    moved = {}
    for tid, comm in n.items():
        want = RAFT if comm == "queen-raft" else LOG if comm in ("queen-raft-log", "queen-raft-sync") else APPLY if tid == main else REST
        try:
            if os.sched_getaffinity(tid) != want:
                os.sched_setaffinity(tid, want)
                moved[(comm, fmt(want))] = moved.get((comm, fmt(want)), 0) + 1
        except Exception:
            pass
    return moved


def say(moved):
    if moved:
        print(time.strftime("%H:%M:%SZ", time.gmtime()), "; ".join(f"{c} x{k} -> {w}" for (c, w), k in sorted(moved.items())), flush=True)


say(one_pass())
while mode == "watch" and os.path.isdir(base):
    time.sleep(1)
    say(one_pass())
