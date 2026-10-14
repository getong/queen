#!/usr/bin/env python3
"""statuspoll.py <address> <out file> [seconds between polls] : this node's raft indexes, many times a second.

One line a poll of /api/v1/raft/status: the time, how long the request took (ms), then the entries proposed,
the last one in this node's own log, the committed one, the applied one, what each follower has
acknowledged (`node:index`), and the index of the last durable point (`D:index`). The differences inside one line are how many entries sat in each stage at that
instant.
"""
import json, sys, time, urllib.request

addr, out = sys.argv[1], sys.argv[2]; every = float(sys.argv[3]) if len(sys.argv) > 3 else 0.1
url = f"http://{addr}:6632/api/v1/raft/status"
f = open(out, "a", buffering=1)
nxt = time.time()
while True:
    t = time.time()
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            d = json.loads(r.read())
        m = " ".join(f"{x['nodeId']}:{x['matchIndex']}" for x in d.get("replication") or [])
        f.write(f"{t:.3f} {(time.time() - t) * 1000:.1f} {d['proposals']} {d['lastLogIndex']} {d['committedIndex']} {d['appliedIndex']} {d.get('state')} {m} D:{d.get('durableIndex')}\n")
    except Exception as e:
        f.write(f"{t:.3f} ERR {e!r}\n")
    nxt += every
    s = nxt - time.time()
    if s > 0:
        time.sleep(s)
    else:
        nxt = time.time()
