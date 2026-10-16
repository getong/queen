#!/usr/bin/env python3
"""readme24_svg.py: the README's Queen chart (assets/bench-queen-e2e-{light,dark}.svg, captured from the docs renderer
on 2026-10-14 for 2.3.0) redrawn with Queen 2.4's runs: e2e p99, the slowest load process's worst 10 s window in
seconds 30 to 60, at 3M msg/s of 256-byte messages (runs/gcp/fm3-*) and 1M msg/s of 1 KB messages (runs/gcp/f1k-*).
At 3M and 1,000 partitions: the median of the five runs of that shape (as the other two charts of this folder).

The captured SVG is rewritten in place of a docs build: same axes (0 to 90 ms, ticks 0 to 80), same x positions; the
two lines, the fourteen points, the two end labels and the alt text change. Its y mapping, read off the capture:
y = 284 - v * (284 - 61.78) / 80.

usage: charts/readme24_svg.py  ->  charts/bench-queen24-e2e-{light,dark}.svg and a PNG of each."""
import glob
import json
import os
import re
import statistics
import subprocess
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(os.path.dirname(BENCH))
GCP = os.path.join(BENCH, "runs", "gcp")
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PARTS = (10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000)
Y0, KY = 284.0, (284 - 61.7777777777778) / 80


def worst(step):
    per = []
    for f in sorted(glob.glob(step + "/q*.json")):
        q = json.load(open(f))
        per.append([w for w in q["windows"] if w.get("offered_per_s", 0) > 0])
    n = min(len(ws) for ws in per)
    picks = [max((ws[i] for ws in per), key=lambda w: w["e2e_p99_ms"]) for i in range(n - 4, n - 1)]
    return max(w["e2e_p99_ms"] for w in picks)


def series():
    s3, s1 = [], []
    for p in PARTS:
        if p == 1000:
            runs = ["bpm1-fix-p1000", "bpm2-fix-p1000", "fm3-p1000-r3000000", "fm3r1-p1000-r3000000", "fm3r2-p1000-r3000000"]
            s3.append(statistics.median(worst(os.path.join(GCP, r, "3000000")) for r in runs))
        else:
            s3.append(worst(os.path.join(GCP, f"fm3-p{p}-r3000000", "3000000")))
        s1.append(worst(os.path.join(GCP, f"f1k-p{p}-r1000000", "1000000")))
    return [round(v) for v in s3], [round(v) for v in s1]


def rewrite(svg, s3, s1):
    xs = [float(x) for x in re.findall(r'<text x="([0-9.]+)" y="301"', svg)]
    assert len(xs) == 7, xs
    ys3 = [Y0 - v * KY for v in s3]
    ys1 = [Y0 - v * KY for v in s1]
    paths = re.findall(r'<path d="[^"]*"', svg)
    assert len(paths) == 2, len(paths)
    svg = svg.replace(paths[0], '<path d="' + " ".join(("M" if i == 0 else "L") + f" {x:.1f} {y:.1f}" for i, (x, y) in
                                                       enumerate(zip(xs, ys3))) + '"', 1)
    svg = svg.replace(paths[1], '<path d="' + " ".join(("M" if i == 0 else "L") + f" {x:.1f} {y:.1f}" for i, (x, y) in
                                                       enumerate(zip(xs, ys1))) + '"', 1)
    circles = re.findall(r'<circle cx="[^"]*" cy="[^"]*"', svg)
    assert len(circles) == 14, len(circles)
    for c, y in zip(circles, ys3 + ys1):
        svg = svg.replace(c, re.sub(r'cy="[^"]*"', f'cy="{y}"', c), 1)
    # end labels: 4 px under the last point, at least 13 px apart
    a, b = ys3[-1] + 4, ys1[-1] + 4
    if abs(a - b) < 13:
        mid = (a + b) / 2
        a, b = (mid - 6.5, mid + 6.5) if a <= b else (mid + 6.5, mid - 6.5)
    svg = re.sub(r'(<text x="703\.48" y=")[0-9.]+("[^>]*>3M msg/s, 256 B</text>)', lambda m: f"{m.group(1)}{a}{m.group(2)}", svg)
    svg = re.sub(r'(<text x="703\.48" y=")[0-9.]+("[^>]*>1M msg/s, 1 KB</text>)', lambda m: f"{m.group(1)}{b}{m.group(2)}", svg)
    old_alt = re.search(r'aria-label="([^"]*)"', svg).group(1)
    alt = ("Queen 2.4 end-to-end p99 against the partition count from 10 to 10,000,000 on three GCP nodes with local NVMe: "
           "at 3,000,000 msg/s of 256-byte messages " + ", ".join(map(str, s3[:-1])) + f" and {s3[-1]} ms; at 1,000,000 msg/s "
           "of 1 KB messages " + ", ".join(map(str, s1[:-1])) + f" and {s1[-1]} ms.")
    return svg.replace(old_alt, alt)


def png(svg_path, png_path, w=820, h=330, bg="#ffffff"):
    """Headless Chrome at 2x, on the page colour the README shows it on, for a look."""
    html = os.path.join(tempfile.mkdtemp(), "v.html")
    open(html, "w").write(f'<html><body style="margin:0;background:{bg}"><img src="file://{svg_path}"></body></html>')
    if os.path.exists(png_path):
        os.remove(png_path)
    with tempfile.TemporaryDirectory() as prof:
        pr = subprocess.Popen([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
                               "--allow-file-access-from-files", f"--user-data-dir={prof}", "--force-device-scale-factor=2",
                               f"--window-size={w},{h}", f"--screenshot={png_path}", "file://" + html],
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
    s3, s1 = series()
    print("3M  ", s3)
    print("1 KB", s1)
    for theme, bg in (("light", "#ffffff"), ("dark", "#0d1117")):
        src = open(os.path.join(REPO, "assets", f"bench-queen-e2e-{theme}.svg")).read()
        out = os.path.join(HERE, f"bench-queen24-e2e-{theme}.svg")
        open(out, "w").write(rewrite(src, s3, s1))
        png(out, os.path.join(HERE, f"bench-queen24-e2e-{theme}.png"), bg=bg)
        print(out)


if __name__ == "__main__":
    main()
