#!/usr/bin/env python3
"""B/msg of two runs side by side, per window: compare.py <a/sample.jsonl> <b/sample.jsonl> <window>..."""
import json
import sys


def load(path):
    rows = []
    for line in open(path):
        try:
            rows.append(json.loads(line))
        except ValueError:
            pass
    return rows


def pushed(r):
    return r.get("m", {}).get("queen_process_push_messages_total", 0.0)


def t0_of(rows):
    for i in range(1, len(rows)):
        if pushed(rows[i]) - pushed(rows[i - 1]) > 1000:
            return rows[i - 1]["t"]
    return rows[0]["t"]


def window(rows, s, e):
    t0 = t0_of(rows)
    sel = [r for r in rows if s <= r["t"] - t0 <= e]
    a, b = sel[0], sel[-1]
    msgs = pushed(b) - pushed(a)
    dt = b["t"] - a["t"]

    def thr(r, n, f):
        return r["thr"].get(n, {}).get(f, 0)

    def fsz(r, k):
        return r["files"].get(k, {}).get("size", 0)

    out = {
        "msg/s": msgs / dt,
        "device (ext4 loop)": (b["blk"]["wsec"] - a["blk"]["wsec"]) * 512 / msgs,
        "process write_bytes": (b["proc"]["write_bytes"] - a["proc"]["write_bytes"]) / msgs,
        "queue-log writers": (thr(b, "queen-qlog-writ", "wb") - thr(a, "queen-qlog-writ", "wb")) / msgs,
        "LMDB checkpoint": (thr(b, "queen-rsm-ckpt", "wchar") - thr(a, "queen-rsm-ckpt", "wchar")) / msgs,
        "qlog files grew": (fsz(b, "qlog") - fsz(a, "qlog")) / msgs,
    }
    return out


def main():
    a, b = load(sys.argv[1]), load(sys.argv[2])
    for w in sys.argv[3:]:
        s, e = (float(x) for x in w.split("-"))
        wa, wb = window(a, s, e), window(b, s, e)
        print(f"\nwindow {w} s{'':24}{'before':>10}{'after':>10}{'change':>10}")
        for k in wa:
            if k == "msg/s":
                print(f"  {k:<34}{wa[k]:>10,.0f}{wb[k]:>10,.0f}")
            else:
                print(f"  {k:<34}{wa[k]:>10.1f}{wb[k]:>10.1f}{wb[k] - wa[k]:>+10.1f}")


if __name__ == "__main__":
    main()
