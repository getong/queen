#!/usr/bin/env python3
"""readme_svg.py: the README's compact chart of the partitions matrix: end to end p99 as one queue grows from 10 to
10M partitions, at five loads with nothing deleted and at 1.5M msg/s with deduplication and deletes on. Light and dark,
for the README's <picture>; system fonts, so the files stay small.

The numbers are read from the runs, as the README's text quotes them:
  - the five clean loads: runs/grid-final.jsonl, mid_e99_worst = the worst load process's 10 s window in seconds
    30-60 of a 70 s load (the statistic of the Kafka and Pulsar reports, and of the README's table);
  - 1.5M msg/s realistic (dedup 60 s, completed messages deleted after 60 s): runs t7-p<P>-r1500000 (t7b- for
    100,000, the run again), the p99 of the last 60 s of 200 s pooled over the load processes (qload -tail), as the
    changelog gives it. Not carried at 10 and 10M partitions: no point there.

Usage: charts/readme_svg.py [outdir]  ->  bench-partitions-p99-{light,dark}.svg (default outdir: the repo's assets/)
and a PNG of each beside this script, for a look."""
import collections, glob, json, math, os, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
FOLDER = os.path.dirname(HERE)
RUNS = os.path.join(FOLDER, "runs")
REPO = os.path.dirname(os.path.dirname(FOLDER))
sys.path.insert(0, os.path.join(FOLDER, "scripts"))
from report import pct  # noqa: E402

OUTDIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(REPO, "assets")
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
W, H = 960, 428  # the method is in the README line under the image
PARTS = (10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000)
TICK = ("10", "100", "1k", "10k", "100k", "1M", "10M")
RATES = (100_000, 500_000, 1_000_000, 1_500_000, 2_000_000)
NAME = {100_000: "100k msg/s", 500_000: "500k", 1_000_000: "1M", 1_500_000: "1.5M", 2_000_000: "2M"}
THEMES = {
    "light": dict(bg="#ffffff", ink="#1f1d1a", mid="#4f4b45", low="#6c675f", grid="#e6e2dc", axis="#c9c3ba",
                  series=("#2a78d6", "#1baf7a", "#c98500", "#eb6834", "#e34948"), real="#6250d6"),
    "dark": dict(bg="#0d1117", ink="#e6edf3", mid="#b9c1ca", low="#8b949e", grid="#21262d", axis="#30363d",
                 series=("#3987e5", "#199e70", "#eda100", "#d95926", "#e66767"), real="#9085e9"),
}
DASH = ("2 4", "7 4", "", "7 4", "")


def clean():
    rows = {}
    for line in open(os.path.join(RUNS, "grid-final.jsonl")):
        line = line.strip()
        if line:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            rows[(r["partitions"], r["rate"])] = r  # a point run again replaces its earlier row
    return {rate: [rows[(p, rate)]["mid_e99_worst"] if (p, rate) in rows and rows[(p, rate)]["carried"] else None
                   for p in PARTS] for rate in RATES}


def realistic():
    out = []
    for p in PARTS:
        tag = "t7b" if p == 100_000 else "t7"
        js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(RUNS, f"{tag}-p{p}-r1500000/1500000/q*.json")))]
        if not js:
            out.append(None)
            continue
        n = min(len(j["windows"]) for j in js)
        last = range(max(0, n - 7), n - 1)  # the last 60 s of load (the last window is the drain)
        pushed = sum(sum(j["windows"][k]["achieved_per_s"] for k in last) / len(last) for j in js)
        popped = sum(sum(j["windows"][k]["popped_per_s"] for k in last) / len(last) for j in js)
        if pushed < 0.995 * 1_500_000 or popped < 0.99 * pushed:
            out.append(None)  # not carried
            continue
        he = collections.Counter()
        for j in js:
            for k, v in j.get("hist_e2e_tail_us", {}).items():
                he[int(k)] += v
        out.append(pct(he, [0.99])[0])
    return out


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render(theme, cl, real):
    t = THEMES[theme]
    o = [f'<rect width="{W}" height="{H}" rx="10" fill="{t["bg"]}"/>']

    def text(x, y, s, size=13, fill=None, anchor="start", weight=400):
        o.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" fill="{fill or t["mid"]}" '
                 f'text-anchor="{anchor}">{esc(s)}</text>')

    # legend: one row, the series in the order of the loads, the realistic line last
    lx, ly = 64, 34
    items = [(NAME[r], t["series"][i], DASH[i], 2.5, "circle") for i, r in enumerate(RATES)]
    items.append(("1.5M, dedup + deletes", t["real"], "", 3.5, "square"))
    for label, col, dash, w, mark in items:
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{lx}" y1="{ly}" x2="{lx + 26}" y2="{ly}" stroke="{col}" stroke-width="{w}"{d}/>')
        if mark == "square":
            o.append(f'<rect x="{lx + 9}" y="{ly - 4}" width="8" height="8" fill="{col}"/>')
        else:
            o.append(f'<circle cx="{lx + 13}" cy="{ly}" r="3.5" fill="{col}"/>')
        text(lx + 33, ly + 4.5, label, 13, t["mid"])
        lx += 33 + 7.6 * len(label) + 22

    X0, X1, Y0, Y1 = 64, 930, 62, 372
    YMAX = 110
    xs = lambda i: X0 + 24 + i * (X1 - X0 - 48) / (len(PARTS) - 1)  # noqa: E731
    ys = lambda v: Y1 - v / YMAX * (Y1 - Y0)  # noqa: E731
    for v in (0, 25, 50, 75, 100):
        o.append(f'<line x1="{X0}" y1="{ys(v):.1f}" x2="{X1}" y2="{ys(v):.1f}" stroke="{t["grid"]}" stroke-width="1"/>')
        text(X0 - 10, ys(v) + 4.5, f"{v}", 12, t["low"], "end")
    text(X0 - 10, Y0 - 12, "ms", 12, t["low"], "end")
    o.append(f'<line x1="{X0}" y1="{Y1}" x2="{X1}" y2="{Y1}" stroke="{t["axis"]}" stroke-width="1"/>')
    for i, lab in enumerate(TICK):
        text(xs(i), Y1 + 20, lab, 12, t["low"], "middle")
    text((X0 + X1) / 2, Y1 + 40, "partitions in one queue", 13, t["mid"], "middle")

    def series(vals, col, dash, w, mark):
        pts = [(xs(i), ys(v)) for i, v in enumerate(vals) if v is not None]
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append('<path d="' + " ".join(("M" if k == 0 else "L") + f"{a:.1f},{b:.1f}" for k, (a, b) in enumerate(pts))
                 + f'" fill="none" stroke="{col}" stroke-width="{w}" stroke-linejoin="round" stroke-linecap="round"{d}/>')
        for a, b in pts:
            if mark == "square":
                o.append(f'<rect x="{a - 4.5:.1f}" y="{b - 4.5:.1f}" width="9" height="9" fill="{col}" '
                         f'stroke="{t["bg"]}" stroke-width="1.5"/>')
            else:
                o.append(f'<circle cx="{a:.1f}" cy="{b:.1f}" r="3.6" fill="{col}" stroke="{t["bg"]}" stroke-width="1.5"/>')

    for i, r in enumerate(RATES):
        series(cl[r], t["series"][i], DASH[i], 2.5, "circle")
    series(real, t["real"], "", 3.5, "square")

    style = ("text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans',Helvetica,Arial,sans-serif;"
             "font-variant-numeric:tabular-nums}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="End to end p99 in milliseconds as one queue grows from 10 to 10 million partitions, at '
            f'100k, 500k, 1M, 1.5M and 2M messages a second, and at 1.5M with deduplication and deletes on">\n'
            f"<style>{style}</style>\n" + "\n".join(o) + "\n</svg>\n")


def png(svg_path, png_path):
    """Headless Chrome at 2x, for a look (the README uses the SVG)."""
    import time
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
    cl, real = clean(), realistic()
    for r in RATES:
        print(NAME[r], [None if v is None else round(v, 1) for v in cl[r]])
    print("1.5M realistic", [None if v is None else round(v, 1) for v in real])
    for theme in ("light", "dark"):
        svg = os.path.join(OUTDIR, f"bench-partitions-p99-{theme}.svg")
        open(svg, "w").write(render(theme, cl, real))
        png(svg, os.path.join(HERE, f"bench-partitions-p99-{theme}.png"))
        print(svg)


if __name__ == "__main__":
    main()
