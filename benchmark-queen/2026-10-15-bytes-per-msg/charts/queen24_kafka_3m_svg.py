#!/usr/bin/env python3
"""queen24_kafka_3m_svg.py: Queen 2.4 (2.3.0 with this folder's fixes 1-3, not released) against Kafka 4.3.1 at 3M
msg/s, one queue or topic from 10 to 10M partitions, on the same six GCP VMs (Kafka's runs of 2026-10-14, XFS and the
tuned thread counts; Queen 2.4's of 2026-10-15). It is 2026-10-14's queen_kafka_3m_svg.py (the docs' Queen vs Kafka
chart) with the Queen rows swapped: the drawing, the Kafka rows and the statistic (worst(): each 10 s window of
seconds 30-60 takes the slowest load process, p50 and p99 the median of the three windows) are that script's own.

Queen 2.4 rows: runs/gcp/fm3-p<P>-r3000000; at 1,000 partitions the median of the five runs of that exact shape
(bpm1-fix, bpm2-fix, fm3, fm3r1, fm3r2). Cores: the three brokers' (gstep.py); memory: the busiest broker's RSS, the
most it reached (sample-n*.log); disk: md0 MB/s a broker, the mean of the three over seconds 30-60.

usage: charts/queen24_kafka_3m_svg.py <the main checkout's 2026-10-12-three-node-new-forms/charts>
  -> charts/queen24-kafka-3m-{light,dark}.svg and a PNG of each."""
import glob
import os
import re
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
GCP = os.path.join(BENCH, "runs", "gcp")
GSTEP = os.path.normpath(os.path.join(BENCH, "..", "2026-10-12-three-node-new-forms", "scripts", "gstep.py"))
ND = os.path.join(BENCH, "scripts", "netdisk_rate.py")

sys.path.insert(0, sys.argv[1])
os.environ["CHART"] = "3m"
import queen_kafka_3m_svg as qk  # noqa: E402

ONE_K = ["bpm1-fix-p1000", "bpm2-fix-p1000", "fm3-p1000-r3000000", "fm3r1-p1000-r3000000", "fm3r2-p1000-r3000000"]


def run_row(run):
    step = os.path.join(GCP, run, "3000000")
    e50, e99 = qk.worst(glob.glob(os.path.join(step, "q*.json")))
    g = subprocess.run(["python3", GSTEP, step, "3000000"], capture_output=True, text=True,
                       env=dict(os.environ, GRID_OUT=os.devnull)).stdout.strip()
    cores = sum(float(x) for x in re.search(r"cores ([0-9.]+) / ([0-9.]+) / ([0-9.]+)", g).groups())
    errs = int(re.search(r"run: shed [0-9,]+ errors ([0-9,]+)", g).group(1).replace(",", ""))
    rss = []
    for n in (1, 2, 3):
        rows = [dict(p.split("=", 1) for p in l.split() if "=" in p) for l in open(os.path.join(step, f"sample-n{n}.log"))]
        rss.append(max(int(r["rss_kb"]) for r in rows if "rss_kb" in r) / 1e6)
    nd = subprocess.run(["python3", ND, "3000000", "30", "60"] + [os.path.join(step, f"net-n{k}.log") for k in (1, 2, 3)],
                        capture_output=True, text=True).stdout
    disk = statistics.mean(float(x) for x in re.findall(r"disk ([0-9.]+) MB/s", nd))
    return dict(carried=g.endswith("OK"), errors=errs, e50=e50, e99=e99, cores=cores, rss=max(rss), disk=disk)


def queen_rows():
    out = {}
    for p in qk.PARTS:
        if p == 1000:
            rs = [run_row(r) for r in ONE_K]
            out[p] = {k: statistics.median(r[k] for r in rs) for k in ("e50", "e99", "cores", "rss", "disk")}
            out[p].update(carried=all(r["carried"] for r in rs), errors=sum(r["errors"] for r in rs),
                          runs=[round(r["e99"]) for r in rs])
        else:
            out[p] = run_row(f"fm3-p{p}-r3000000")
    return out


def main():
    Q = queen_rows()
    K = {p: r for p in qk.PARTS if (r := qk.kafka_row(p)) is not None}
    for p in qk.PARTS:
        q, k = Q[p], K.get(p)
        print(f"{p:>9} Queen 2.4 {q['e50']:6.1f} / {q['e99']:7.1f} cores {q['cores']:5.1f} rss {q['rss']:4.1f} disk "
              f"{q['disk']:4.0f} | Kafka " + (f"{k['e50']:6.1f} / {k['e99']:7.1f} cores {k['cores']:5.1f} carried "
                                               f"{k['carried']}" if k else "-"))
    one_k = ", ".join(str(v) for v in sorted(Q[1000]["runs"]))
    qk.FOOT_KAFKA[2] = ("Queen 2.4: 2.3.0 plus three changes that cut the bytes written a message, not released; XFS noatime; "
                        "binary push, zstd, counted acks.")
    qk.FOOT_KAFKA[3] = ("A Queen write is answered after the leader's fsync and one follower's; framed pops; dedup off; 10M with "
                        "QUEEN_CONSUME_IDLE_UNLOAD_S=30.")
    qk.FOOT_KAFKA[4] = (f"Queen at 1,000 partitions: the median of five runs (their p99: {one_k} ms). Kafka's memory leaves "
                        "out the page cache it reads from.")
    qk.H = 872
    qk.base.W, qk.base.H = qk.W, qk.H
    for theme in ("light", "dark"):
        svg = qk.render(theme, Q, K)
        svg = svg.replace("Queen 2.3.0 and Kafka 4.3.1 at", "Queen 2.4 and Kafka 4.3.1 at")
        sub = ("The same six GCP VMs: three c4-standard-24-lssd brokers (24 vCPU, 88 GB, four local NVMe disks in RAID0), "
               "three 16-vCPU loaders · 2026-10-14")
        assert sub in svg, "the subtitle changed in queen_kafka_3m_svg.py"
        svg = svg.replace(sub, "Three GCP c4-standard-24-lssd brokers (24 vCPU, 88 GB, four NVMe disks in RAID0) and three 16-vCPU "
                               "loaders · Kafka 10-14, Queen 10-15")
        path = os.path.join(HERE, f"queen24-kafka-3m-{theme}.svg")
        open(path, "w").write(svg)
        qk.png(path, os.path.join(HERE, f"queen24-kafka-3m-{theme}.png"))
        print(path)


if __name__ == "__main__":
    main()
