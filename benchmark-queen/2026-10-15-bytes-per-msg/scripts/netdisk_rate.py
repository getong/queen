#!/usr/bin/env python3
"""Disk and network rates per node from netdisk.sh logs (one line per second).

usage: netdisk_rate.py <msgs_per_s> <start_line> <end_line> net-n1.log [net-n2.log ...]
"""
import sys


def parse(path):
    rows = []
    with open(path) as f:
        for line in f:
            d = {}
            for tok in line.split():
                if "=" in tok:
                    k, v = tok.split("=", 1)
                    try:
                        d[k] = float(v)
                    except ValueError:
                        pass
            if "t" in d and "wsec" in d:
                rows.append(d)
    return rows


def main():
    rate = float(sys.argv[1])
    s, e = int(sys.argv[2]), int(sys.argv[3])
    for path in sys.argv[4:]:
        rows = parse(path)
        ee = min(e, len(rows) - 1)
        a, b = rows[s], rows[ee]
        dt = b["t"] - a["t"]
        disk = (b["wsec"] - a["wsec"]) * 512 / dt
        rx = (b["rx"] - a["rx"]) / dt
        tx = (b["tx"] - a["tx"]) / dt
        wios = (b["wios"] - a["wios"]) / dt
        fios = (b.get("fios", 0) - a.get("fios", 0)) / dt
        print(
            f"{path}: lines {len(rows)} window {s}-{ee} ({dt:.0f} s): disk {disk/1e6:.1f} MB/s "
            f"= {disk/rate:.1f} B/msg | writes {wios:.0f}/s ({disk/max(wios,1)/1024:.1f} KiB each) "
            f"flushes {fios:.0f}/s | rx {rx/1e6:.1f} tx {tx/1e6:.1f} MB/s"
        )


if __name__ == "__main__":
    main()
