#!/usr/bin/env python3
"""One table at 3M x 256 B, docs statistic for every system: Queen 2.4 (10-15 fm3 + today's 1k anchor), Kafka XFS+t24
(10-14 + today's anchor), Pulsar and Redpanda (today). Cores: Queen/Kafka/Pulsar = /proc of the broker processes,
Redpanda = shards busy (Seastar polls)."""
import glob, json, os, re, statistics, sys
sys.path.insert(0, os.path.dirname(__file__))
from sum1016 import stat, other  # noqa
KP = "/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar"
B = "/Users/alice/Work/queen/benchmark-queen/2026-10-15-bytes-per-msg/runs/gcp"
QR = "/Users/alice/Work/queen/benchmark-queen/2026-10-12-three-node-new-forms/runs"
def busy(d):
    s, e = {}, {}
    for l in open(os.path.join(d, "busy.txt")):
        m = re.match(r'(start|end) node=(\d) t=([\d.]+) busy_s=([\d.]+)', l)
        if m: (s if m[1] == 'start' else e)[m[2]] = (float(m[3]), float(m[4]))
    return sum((e[n][1] - s[n][1]) / (e[n][0] - s[n][0]) for n in s)
qrows = {json.loads(l)["partitions"]: json.loads(l) for l in open(os.path.join(B, "grid-fix-m3.jsonl"))}
anchor = [json.loads(l) for l in open(os.path.join(QR, "grid-gcp-1016.jsonl"))][-1]
out = []
for P in (10, 100, 1000, 10000):
    row = {"P": P}
    if P == 1000:
        q = anchor; qd = os.path.join(QR, q["dir"])
    else:
        q = qrows[P]; qd = os.path.join(B, q["dir"].split("/")[0])
        qd = os.path.join(qd, q["dir"].split("/")[1]) if "/" in q["dir"] else qd
    s = stat(glob.glob(os.path.join(qd, "q*.json")))
    row["queen"] = dict(carried=q["carried"], e50=s[0], e99=s[1], cores=sum(q["cores"]), src=q["dir"])
    kt = f"1x{P}-3000k-gcp24-xfs-t24" + ("-1016" if P == 1000 else "")
    for name, sysn, tag in (("kafka", "kafka", kt), ("pulsar", "pulsar", f"1x{P}-3000k-gcp24"),
                            ("redpanda", "redpanda", f"1x{P}-3000k-gcp24")):
        r = other(sysn, tag)
        if name == "redpanda":
            r["cores"] = busy(os.path.join(KP, "runs", "redpanda", tag))
        row[name] = r
    out.append(row)
print(f"{'part':>6} | " + " | ".join(f"{n:^24}" for n in ("Queen 2.4", "Kafka", "Pulsar", "Redpanda")))
for row in out:
    cells = []
    for n in ("queen", "kafka", "pulsar", "redpanda"):
        r = row[n]
        c = "" if r.get("carried") else " NC"
        cells.append(f"{r['e50']:6.1f}/{r['e99']:6.1f} {r['cores']:5.1f}c{c:3}")
    print(f"{row['P']:>6} | " + " | ".join(f"{c:^24}" for c in cells))
json.dump(out, open(os.path.join(os.path.dirname(__file__), "full1016.json"), "w"), indent=1, default=str)
