#!/usr/bin/env python3
"""m3_23_vs_24_svg.py: Queen 2.3 against 2.4 at 3M msg/s on three GCP c4-standard-24-lssd brokers, one queue from 10
to 10M partitions (2.4 = 2.3.0 with fixes 1-3 of this folder, not released). Top: end to end latency p50 and p99 of
both versions (the docs' figure). Under it: the three brokers' CPU, the leader's memory, each broker's disk writes and
the disk's time a write. Same layout and fonts as ../../2026-10-12-three-node-new-forms/charts/gcp24_3m_svg.py; light
and dark; each point carries a <title>, so a browser shows its values on hover.

Reads charts/m3-23-vs-24.json (charts/m3data.py). Usage: charts/m3_23_vs_24_svg.py -> charts/queen-3m-23-vs-24-
{light,dark}.svg and a PNG of each, for a look."""
import json
import os
import statistics
import subprocess
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
W, H = 960, 790
TICK = {10: "10", 100: "100", 1000: "1k", 10_000: "10k", 100_000: "100k", 1_000_000: "1M", 10_000_000: "10M"}
# Queen blue for 2.4 (her pick of 2026-10-13), a warm grey for 2.3; p99 the heavy shade and solid, p50 the light
# shade and dashed.
THEMES = {
    "light": dict(bg="#ffffff", ink="#1f1d1a", mid="#4f4b45", low="#6c675f", grid="#e6e2dc", axis="#c9c3ba",
                  v24_99="#2b6cd4", v24_50="#6e9be3", v23_99="#6c675f", v23_50="#a39d94"),
    "dark": dict(bg="#0d1117", ink="#e6edf3", mid="#b9c1ca", low="#8b949e", grid="#21262d", axis="#30363d",
                 v24_99="#5a96f5", v24_50="#3a6cbf", v23_99="#b1b9c2", v23_50="#6e7781"),
}
LAT = (("2.4 p99", "2.4", "e2e_p99", "v24_99", "", 3.0), ("2.4 p50", "2.4", "e2e_p50", "v24_50", "6 4", 2.4),
       ("2.3 p99", "2.3", "e2e_p99", "v23_99", "", 2.6), ("2.3 p50", "2.3", "e2e_p50", "v23_50", "6 4", 2.4))


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render(theme, data):
    t = THEMES[theme]
    parts = data["partitions"]
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

    def pt(ver, p):
        return data[ver][str(p)]

    # heading
    text(40, 38, "Queen 2.3 and 2.4 at 3M msg/s: one queue, 10 to 10M partitions", 18, t["ink"], weight=600)
    text(40, 60, "Three GCP c4-standard-24-lssd brokers (24 vCPU, 88 GB, four local NVMe disks in RAID0) and three 16-vCPU "
                 "loaders", 12.5, t["low"])

    # legend
    lx, ly = 40, 96
    for label, _, _, key, dash, w in LAT:
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{lx}" y1="{ly}" x2="{lx + 28}" y2="{ly}" stroke="{t[key]}" stroke-width="{w}"{d}/>')
        o.append(f'<circle cx="{lx + 14}" cy="{ly}" r="3.5" fill="{t[key]}"/>')
        text(lx + 35, ly + 4.5, label, 13, t["mid"])
        lx += 35 + 7.4 * len(label) + 24
    text(lx + 4, ly + 4.5, "end to end latency = pushed until consumed", 12, t["low"])

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
    text((X0 + X1) / 2, Y1 + 46, "partitions in one queue (each step is 10 times the one before)", 13, t["mid"], "middle")
    ends = []
    for label, ver, field, key, dash, w in reversed(LAT):  # 2.4 p99 drawn last, on top
        pts = [(xs(i), ys(pt(ver, p)[field])) for i, p in enumerate(parts)]
        line(pts, t[key], dash, w)
        for i, (a, b) in enumerate(pts):
            r = pt(ver, parts[i])
            tip = (f"{TICK[parts[i]]} partitions · Queen {ver} · e2e p50 {r['e2e_p50']:.1f} ms, p99 {r['e2e_p99']:.1f} ms "
                   f"(worst window {r['e2e_worst']:.0f}) · push p50 {r['push_p50']:.1f}, p99 {r['push_p99']:.1f} ms")
            if "runs" in r:
                tip += " · median of " + ", ".join(f"{x['e2e_p50']:.1f}/{x['e2e_p99']:.1f}" for x in r["runs"])
            dot(a, b, t[key], 3.8 if field == "e2e_p99" else 3.2, tip)
        ends.append([pts[-1][1], label, t[key], pt(ver, parts[-1])[field]])
    ends.sort()
    for k in range(1, len(ends)):  # end labels at least 14 px apart
        ends[k][0] = max(ends[k][0], ends[k - 1][0] + 14)
    for yy, label, col, v in ends:
        text(X1 - 6, yy + 4, f"{label} {v:.0f}", 11.5, t["mid"])

    # broker panels
    py = 448
    text(40, py, "The brokers", 14, t["ink"], weight=600)
    lx = 150
    for label, key, dash in (("2.4", "v24_99", ""), ("2.3", "v23_99", "6 4")):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{lx}" y1="{py - 4.5}" x2="{lx + 28}" y2="{py - 4.5}" stroke="{t[key]}" stroke-width="2.4"{d}/>')
        o.append(f'<circle cx="{lx + 14}" cy="{py - 4.5}" r="3.5" fill="{t[key]}"/>')
        text(lx + 35, py, label, 13, t["mid"])
        lx += 35 + 7.4 * len(label) + 30

    def value(r, what):
        if what == "cores":
            return sum(r["cores"]), " / ".join(f"{c:.1f}" for c in r["cores"])
        if what == "mem":
            return r["mem_gb"][0], " / ".join(f"{c:.1f}" for c in r["mem_gb"])
        if what == "disk":
            return statistics.mean(r["disk_mb"]), " / ".join(f"{c:,.0f}" for c in r["disk_mb"]) + \
                f" ({statistics.mean(r['disk_bpm']):.0f} B a message)"
        return statistics.mean(r["disk_ms"]), " / ".join(f"{c:.3f}" for c in r["disk_ms"])

    panels = (("CPU, cores (three brokers)", "cores", 30, (0, 10, 20, 30), "{:.0f}", "{:.1f} cores", "n1 / n2 / n3 ", None),
              ("Memory, GB (the leader)", "mem", 30, (0, 10, 20, 30), "{:.0f}", "{:.1f} GB", "n1 / n2 / n3 ", None),
              ("Disk writes a broker, MB/s", "disk", 1400, (0, 500, 1000), "{:,.0f}", "{:,.0f} MB/s", "n1 / n2 / n3 ", 1340),
              ("Disk time a write, ms", "ms", 0.6, (0, 0.2, 0.4, 0.6), "{:.1f}", "{:.3f} ms", "n1 / n2 / n3 ", None))
    PW, GAP, PX = 178, 50, 64
    PY0, PY1 = 488, 612
    for k, (title, what, ymax, grid, fmt, tipfmt, tiplead, cap) in enumerate(panels):
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
        for ver, key, dash in (("2.3", "v23_99", "6 4"), ("2.4", "v24_99", "")):
            vals = [value(pt(ver, p), what) for p in parts]
            pts = [(pxs(i), pys(v)) for i, (v, _) in enumerate(vals)]
            line(pts, t[key], dash, 2.2)
            for i, (a, b) in enumerate(pts):
                v, each = vals[i]
                dot(a, b, t[key], 3.0, f"{TICK[parts[i]]} partitions · Queen {ver} · {tipfmt.format(v)} · {tiplead}{each}")
    text(PX + 2 * (PW + GAP) - 34 + 2, PY1 + 34, "partitions in one queue", 12, t["low"], "middle")

    # method
    k23 = sorted(x["e2e_p99"] for x in pt("2.3", 1000)["runs"])
    k24 = sorted(x["e2e_p99"] for x in pt("2.4", 1000)["runs"])
    fy = 692
    for s in ("2.4 = 2.3.0 plus three changes that cut the bytes written a message (not released). 2.3 measured on "
              "2026-10-14, 2.4 on 2026-10-15.",
              "A fresh three-node cluster for each point; 100 messages of 256 B a request (binary push, zstd), counted acks, "
              "framed pops; dedup off, nothing deleted.",
              "Latency: the slowest of nine load processes, the median of its three 10 s windows in seconds 30-60 (the docs' "
              "figure). Disk: /dev/md0, seconds 30-60.",
              "1,000 partitions: five runs of each version, the median drawn (p99 of 2.4's runs "
              + ", ".join(f"{v:.0f}" for v in k24) + " ms; of 2.3's " + ", ".join(f"{v:.0f}" for v in k23) + " ms).",
              "10M partitions ran with QUEEN_CONSUME_IDLE_UNLOAD_S=30. 2.3 at 100k: one pop of millions timed out (HTTP 503)."):
        text(40, fy, s, 11.5, t["low"])
        fy += 18
    style = ("text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans',Helvetica,Arial,sans-serif;"
             "font-variant-numeric:tabular-nums}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="Queen 2.3 and 2.4 at 3 million messages a second on one queue of 10 to 10 million partitions: end '
            f'to end latency, CPU, leader memory, disk writes and disk time a write">\n'
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
    data = json.load(open(os.path.join(HERE, "m3-23-vs-24.json")))
    for theme in ("light", "dark"):
        svg = os.path.join(HERE, f"queen-3m-23-vs-24-{theme}.svg")
        open(svg, "w").write(render(theme, data))
        png(svg, os.path.join(HERE, f"queen-3m-23-vs-24-{theme}.png"))
        print(svg)


if __name__ == "__main__":
    main()
