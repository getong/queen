#!/usr/bin/env python3
"""gcp24_3m_svg.py: the 3M msg/s matrix of 2026-10-14 on three GCP c4-standard-24-lssd brokers, one queue from 10 to 10M
partitions. Top: push and end to end latency (p50, p99). Under it, per node (the leader n1 against the two followers):
CPU cores, broker memory, disk writes and the disk's time a write. Light and dark; system fonts; each point carries a
<title>, so a browser shows its value on hover.

Reads runs/matrix-gcp24-3m.json (written by the session's m3sum.py from runs/grid-gcp.jsonl and each point's
net-n*.log and sample-n*.log): latency = the nine load processes pooled over the last 30 s of a 70 s step; cores = the
harness rows (same 30 s); memory = the broker's RSS, the most it reached around that window; disk = /dev/md0, mean of
the per-second values over the last 30 s of load (ms a write: the median second).

Usage: charts/gcp24_3m_svg.py  ->  charts/gcp24-3m-{light,dark}.svg and a PNG of each, for a look."""
import json, os, subprocess, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
RUNS = os.path.join(os.path.dirname(HERE), "runs")
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
W, H = 960, 762
TICK = {10: "10", 100: "100", 1000: "1k", 10_000: "10k", 100_000: "100k", 1_000_000: "1M", 10_000_000: "10M"}
# Categorical slots checked with the dataviz validator (light on #ffffff, dark on #0d1117): all checks pass; light has
# a contrast WARN on green and amber, which the legend, the dash patterns and the end labels answer.
THEMES = {
    "light": dict(bg="#ffffff", ink="#1f1d1a", mid="#4f4b45", low="#6c675f", grid="#e6e2dc", axis="#c9c3ba",
                  push50="#1baf7a", push99="#c98500", e2e50="#2a78d6", e2e99="#e34948", leader="#6250d6", foll="#8a857d"),
    "dark": dict(bg="#0d1117", ink="#e6edf3", mid="#b9c1ca", low="#8b949e", grid="#21262d", axis="#30363d",
                 push50="#199e70", push99="#b98000", e2e50="#3987e5", e2e99="#e66767", leader="#9085e9", foll="#8b949e"),
}
LAT = (("push p50", "push_p50", "push50", "2 4", 2.2), ("push p99", "push_p99", "push99", "7 4", 2.2),
       ("e2e p50", "e2e_p50", "e2e50", "", 2.2), ("e2e p99", "e2e_p99", "e2e99", "", 3.0))


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render(theme, rows):
    t = THEMES[theme]
    parts = [r["partitions"] for r in rows]
    o = [f'<rect width="{W}" height="{H}" rx="10" fill="{t["bg"]}"/>']

    def text(x, y, s, size=13, fill=None, anchor="start", weight=400):
        o.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" fill="{fill or t["mid"]}" '
                 f'text-anchor="{anchor}">{esc(s)}</text>')

    def line(pts, col, dash, w):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append('<path d="' + " ".join(("M" if k == 0 else "L") + f"{a:.1f},{b:.1f}" for k, (a, b) in enumerate(pts))
                 + f'" fill="none" stroke="{col}" stroke-width="{w}" stroke-linejoin="round" stroke-linecap="round"{d}/>')

    def dot(x, y, col, r, tip):
        o.append(f'<g><title>{esc(tip)}</title><circle cx="{x:.1f}" cy="{y:.1f}" r="12" fill="transparent"/>'
                 f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{col}" stroke="{t["bg"]}" stroke-width="1.5"/></g>')

    # heading: the load and the machines (the method is in the lines at the foot)
    text(40, 38, "Queen 2.3.0 at 3M msg/s: one queue, 10 to 10M partitions", 18, t["ink"], weight=600)
    text(40, 60, "Three GCP c4-standard-24-lssd brokers (24 vCPU, 88 GB, four local NVMe disks in RAID0), "
                 "three 16-vCPU loaders · 2026-10-14", 12.5, t["low"])

    # latency legend
    lx, ly = 40, 96
    for label, _, key, dash, w in LAT:
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{lx}" y1="{ly}" x2="{lx + 26}" y2="{ly}" stroke="{t[key]}" stroke-width="{w + 0.3}"{d}/>')
        o.append(f'<circle cx="{lx + 13}" cy="{ly}" r="3.5" fill="{t[key]}"/>')
        text(lx + 33, ly + 4.5, label, 13, t["mid"])
        lx += 33 + 7.4 * len(label) + 26
    text(lx + 4, ly + 4.5, "end to end = pushed until consumed", 12, t["low"])

    # latency panel
    X0, X1, Y0, Y1, YMAX = 64, 860, 128, 372, 80
    xs = lambda i: X0 + 24 + i * (X1 - X0 - 48) / (len(parts) - 1)  # noqa: E731
    ys = lambda v: Y1 - v / YMAX * (Y1 - Y0)  # noqa: E731
    for v in (0, 20, 40, 60, 80):
        o.append(f'<line x1="{X0}" y1="{ys(v):.1f}" x2="{X1}" y2="{ys(v):.1f}" stroke="{t["grid"]}" stroke-width="1"/>')
        text(X0 - 10, ys(v) + 4.5, f"{v}", 12, t["low"], "end")
    text(X0 - 10, Y0 - 12, "ms", 12, t["low"], "end")
    o.append(f'<line x1="{X0}" y1="{Y1}" x2="{X1}" y2="{Y1}" stroke="{t["axis"]}" stroke-width="1"/>')
    for i, p in enumerate(parts):
        text(xs(i), Y1 + 20, TICK[p], 12, t["low"], "middle")
        if not rows[i]["carried"]:
            text(xs(i), Y1 + 35, "1 pop timed out", 10.5, t["low"], "middle")
    text((X0 + X1) / 2, Y1 + 52, "partitions in one queue (each step is 10 times the one before)", 13, t["mid"], "middle")
    ends = []
    for label, field, key, dash, w in LAT:
        pts = [(xs(i), ys(r[field])) for i, r in enumerate(rows)]
        line(pts, t[key], dash, w)
        for i, (a, b) in enumerate(pts):
            dot(a, b, t[key], 3.6, f"{TICK[parts[i]]} partitions · {label} {rows[i][field]:.1f} ms")
        ends.append([pts[-1][1], label, t[key], rows[-1][field]])
    ends.sort()
    for k in range(1, len(ends)):  # keep the end labels 14 px apart
        ends[k][0] = max(ends[k][0], ends[k - 1][0] + 14)
    for yy, label, col, v in ends:
        text(X1 - 8, yy + 4, f"{label} {v:.0f}", 11.5, t["mid"])

    # per node panels
    py = 446
    text(40, py, "Each node", 14, t["ink"], weight=600)
    lx = 128
    for label, key, dash in (("leader (n1)", "leader", ""), ("followers (n2, n3: mean)", "foll", "6 4")):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{lx}" y1="{py - 4.5}" x2="{lx + 26}" y2="{py - 4.5}" stroke="{t[key]}" stroke-width="2.4"{d}/>')
        o.append(f'<circle cx="{lx + 13}" cy="{py - 4.5}" r="3.5" fill="{t[key]}"/>')
        text(lx + 33, py, label, 13, t["mid"])
        lx += 33 + 7.4 * len(label) + 26

    def per_node(r, what):
        if what == "cores":
            v = r["cores"]
        elif what == "mem":
            v = [r["mem"][str(n)]["rss_gb"] for n in (1, 2, 3)]
        elif what == "disk":
            v = [r["disk"][str(n)]["mb"] for n in (1, 2, 3)]
        else:
            v = [r["disk"][str(n)]["ms"] for n in (1, 2, 3)]
        return v[0], (v[1] + v[2]) / 2, v

    panels = (("CPU, cores of 24", "cores", 24, (0, 8, 16, 24), "{:.0f}", "{:.1f} cores", None),
              ("Memory, GB of 88 (broker RSS)", "mem", 30, (0, 10, 20, 30), "{:.0f}", "{:.1f} GB", None),
              ("Disk writes, MB/s", "disk", 1400, (0, 500, 1000), "{:,.0f}", "{:,.0f} MB/s", 1340),
              ("Disk time a write, ms", "ms", 1.0, (0, 0.5, 1.0), "{:.1f}", "{:.2f} ms", None))
    PW, GAP, PX = 178, 50, 64
    PY0, PY1 = 486, 610
    for k, (title, what, ymax, grid, fmt, tipfmt, cap) in enumerate(panels):
        x0 = PX + k * (PW + GAP)
        x1 = x0 + PW
        text(x0 - 34, PY0 - 16, title, 12.5, t["mid"], weight=600)
        pxs = lambda i: x0 + 8 + i * (PW - 16) / (len(parts) - 1)  # noqa: E731
        pys = lambda v: PY1 - v / ymax * (PY1 - PY0)  # noqa: E731
        for g in grid:
            o.append(f'<line x1="{x0}" y1="{pys(g):.1f}" x2="{x1}" y2="{pys(g):.1f}" stroke="{t["grid"]}" stroke-width="1"/>')
            text(x0 - 6, pys(g) + 4, fmt.format(g), 10.5, t["low"], "end")
        o.append(f'<line x1="{x0}" y1="{PY1}" x2="{x1}" y2="{PY1}" stroke="{t["axis"]}" stroke-width="1"/>')
        if cap:
            o.append(f'<line x1="{x0}" y1="{pys(cap):.1f}" x2="{x1}" y2="{pys(cap):.1f}" stroke="{t["low"]}" '
                     f'stroke-width="1.2" stroke-dasharray="3 3"/>')
            text(x1, pys(cap) - 5, f"RAID0 limit {cap:,}", 10.5, t["low"], "end")
        for i, p in enumerate(parts):
            if p in (10, 1000, 100_000, 10_000_000):
                text(pxs(i), PY1 + 16, TICK[p], 10.5, t["low"], "middle")
        for idx, key, dash in ((0, "leader", ""), (1, "foll", "6 4")):  # followers on top: where they match, the leader shows through the dashes
            vals = [per_node(r, what) for r in rows]
            pts = [(pxs(i), pys(v[idx])) for i, v in enumerate(vals)]
            line(pts, t[key], dash, 2.2)
            for i, (a, b) in enumerate(pts):
                n1, _, v3 = vals[i]
                tip = (f"{TICK[parts[i]]} partitions · n1 " + " · n2 ".join([tipfmt.format(v3[0]), tipfmt.format(v3[1])])
                       + " · n3 " + tipfmt.format(v3[2]))
                dot(a, b, t[key], 3.0, tip)
    text(PX + 2 * (PW + GAP) - 34 + 2, PY1 + 34, "partitions in one queue", 12, t["low"], "middle")

    # method
    fy = 682
    for s in ("A fresh three-node cluster for each point; 100 messages of 256 B a request (binary push, zstd), counts acks, "
              "framed pops; dedup off, nothing deleted.",
              "Latency: nine load processes pooled, last 30 s of a 70 s step. Disk: /dev/md0 each second, mean of the last 30 s. "
              "Memory: the most the broker held.",
              "10M partitions ran with QUEEN_CONSUME_IDLE_UNLOAD_S=30.",
              "100k: every number carried, but one pop of millions timed out (HTTP 503), so the harness marks the point not carried."):
        text(40, fy, s, 11.5, t["low"])
        fy += 18
    style = ("text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans',Helvetica,Arial,sans-serif;"
             "font-variant-numeric:tabular-nums}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="Queen 2.3.0 at 3 million messages a second on one queue of 10 to 10 million partitions: push and '
            f'end to end latency, and per node CPU, memory, disk writes and disk time a write">\n'
            f"<style>{style}</style>\n" + "\n".join(o) + "\n</svg>\n")


def png(svg_path, png_path):
    """Headless Chrome at 2x, for a look."""
    if os.path.exists(png_path):
        os.remove(png_path)
    with tempfile.TemporaryDirectory() as prof:
        pr = subprocess.Popen([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
                               f"--user-data-dir={prof}", "--force-device-scale-factor=2", f"--window-size={W},{H}",
                               f"--screenshot={png_path}", "file://" + svg_path],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        size, deadline = -1, time.time() + 60
        while time.time() < deadline and pr.poll() is None:
            time.sleep(0.5)
            if os.path.exists(png_path) and os.path.getsize(png_path) == size > 0:
                break
            size = os.path.getsize(png_path) if os.path.exists(png_path) else -1
        if pr.poll() is None:
            pr.terminate()


def main():
    rows = sorted(json.load(open(os.path.join(RUNS, "matrix-gcp24-3m.json"))), key=lambda r: r["partitions"])
    for theme in ("light", "dark"):
        svg = os.path.join(HERE, f"gcp24-3m-{theme}.svg")
        open(svg, "w").write(render(theme, rows))
        png(svg, os.path.join(HERE, f"gcp24-3m-{theme}.png"))
        print(svg)


if __name__ == "__main__":
    main()
