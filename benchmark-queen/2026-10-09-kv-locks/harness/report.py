#!/usr/bin/env python3
"""report.py [runs dir] [--fill README.md] — the tables of ../README.md.

Every table of the README comes out of this script: a row is one run's
`load.txt` ([json] line) and `broker.txt`, and a run that is missing prints no
row. `--fill` rewrites the README's `<!-- T:name -->` blocks in place.
"""
import glob
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from table import load  # noqa: E402

ROOT = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "runs"
S, C = "single", "cluster3"


def run(topo, tag):
    return load(os.path.join(ROOT, topo, tag))


def n(v, d=0):
    return f"{v:,.{d}f}"


def ms(v):
    return f"{v:.2f}" if v < 10 else f"{v:.1f}" if v < 100 else f"{v:,.0f}"


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" if i == 0 else "--:" for i in range(len(head))) + "|"]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows if row]
    return "\n".join(out)


# ---- KV and anything else whose headline is one series named `call`

KV_HEAD = ["", "connections", "ops/s", "p50 ms", "p99 ms", "p99.9 ms", "broker cores", "idle cores"]


def kv_row(label, topo, tag):
    r = run(topo, tag)
    if not r:
        return None
    s, b = r["series"]["call"], r["broker"]
    return [label, n(r["workers"]), n(r["opsPerSec"]), ms(s["p50"]), ms(s["p99"]), ms(s["p999"]),
            n(b.get("brokerCores", 0), 1), n(b.get("hostIdle", 0), 1)]


def kv_table(topo, rows):
    return table(KV_HEAD, [kv_row(label, topo, tag) for label, tag in rows])


def paced_row(label, topo, tag):
    r = run(topo, tag)
    if not r:
        return None
    s, b = r["series"]["call"], r["broker"]
    return [label, n(r["opsPerSec"]), ms(s["p50"]), ms(s["p90"]), ms(s["p99"]), ms(s["p999"]), ms(r["maxLagMs"]),
            n(b.get("brokerCores", 0), 1)]


PACED_HEAD = ["", "ops/s carried", "p50 ms", "p90 ms", "p99 ms", "p99.9 ms", "furthest behind schedule, ms",
              "broker cores"]

# ---- Locks


def cycle_row(label, topo, tag):
    r = run(topo, tag)
    if not r:
        return None
    a, rel, c, b = r["series"]["acquire"], r["series"]["release"], r["counters"], r["broker"]
    secs = r["seconds"]
    return [label, n(r["workers"]), r["batch"], n(c["acquired"] / secs), ms(a["p50"]), ms(a["p99"]), ms(rel["p50"]),
            ms(rel["p99"]), n(b.get("brokerCores", 0), 1), n(b.get("hostIdle", 0), 1)]


CYCLE_HEAD = ["", "connections", "locks per call", "acquire + release pairs/s", "acquire p50 ms", "p99 ms",
              "release p50 ms", "p99 ms", "broker cores", "idle cores"]


def contend_row(label, topo, tag):
    r = run(topo, tag)
    if not r:
        return None
    w, t, c, b = r["series"]["wait"], r["series"]["try"], r["counters"], r["broker"]
    secs = r["seconds"]
    return [label, n(c["acquired"] / secs), ms(w["p50"]), ms(w["p99"]), ms(w["maxMs"]), n(c["tries"] / secs),
            n(c["refused"] / secs), n(c["contended"] / secs), ms(t["p50"]), n(b.get("brokerCores", 0), 1)]


CONTEND_HEAD = ["", "acquired/s", "wait p50 ms", "wait p99 ms", "longest wait ms", "acquire calls/s",
                "answered `held`/s", "answered `contended`/s", "call p50 ms", "broker cores"]

# ---- What bounds a write


def bound_row(label, topo, tag):
    r = run(topo, tag)
    if not r:
        return None
    b = r["broker"]
    pct = lambda v: f"{v * 100:.0f}%"  # noqa: E731
    return [label, n(r["workers"]), n(r["opsPerSec"]), pct(b["loopBusy"]), n(b["planner"], 2), pct(b["logBusy"]),
            pct(b["applyBusy"]), pct(b["ckptBusy"]), n(b.get("diskMB", 0)), n(b.get("hostIdle", 0), 1)]


BOUND_HEAD = ["", "connections", "ops/s", "planning loop busy", "of which planner thread CPU, cores",
              "log writer busy", "apply thread busy", "store checkpoint busy", "disk MB/s", "idle cores"]


def guard_pivot():
    shapes = [("KV put", "kvput"), ("KV put behind a guard", "kvput-guard"), ("transaction: 1 push", "txn-push"),
              ("transaction: 1 push + guard", "txn-push-guard"), ("transaction: 1 push + KV put", "txn-push-kv"),
              ("transaction: 1 push + KV put + guard", "txn-push-kv-guard")]
    head = [""]
    for w in (64, 256, 1024):
        head += [f"{w:,} connections: per s", "p50 ms", "planner thread, cores"]
    rows = []
    for label, tag in shapes:
        row = [label]
        for w in (64, 256, 1024):
            r = run(S, f"{tag}-w{w}")
            if not r:
                row += ["", "", ""]
                continue
            row += [n(r["opsPerSec"]), ms(r["series"]["call"]["p50"]), n(r["broker"]["planner"], 2)]
        rows.append(row)
    return table(head, rows)


def totals():
    runs = errs = acq = two = back = 0
    for f in sorted(glob.glob(os.path.join(ROOT, "*", "*", "load.txt"))):
        for line in open(f):
            if line.startswith("[json] "):
                r = json.loads(line[7:])
                runs += 1
                errs += r["errTotal"]
                c = r["counters"]
                if "twoHolders" in c:
                    acq += c["acquired"]
                    two += c["twoHolders"]
                    back += c["tokenWentBack"]
    starts = []
    for f in glob.glob(os.path.join(ROOT, "matrix-*.log")):
        starts += re.findall(r"^=== (\d\d:\d\d:\d\d)", open(f).read(), re.M)
    return (f"{runs} points, the first started at {min(starts)[:5]}Z and the last at {max(starts)[:5]}Z. Calls that "
            f"failed, over all of them: {errs}. Acquisitions under contention: {int(acq):,}; with two holders at "
            f"once: {int(two)}; with a token lower than one seen before: {int(back)}.")


TABLES = {
    "totals": totals,
    "kv-fresh": lambda: kv_table(S, [("mix", f"kv-mix-b1-w{w}") for w in (16, 64, 256, 1024, 4096)]
                                 + [("`incr` only", f"kv-incr-b1-w{w}") for w in (64, 256, 1024)]
                                 + [("`putIfAbsent` only", f"kv-pia-b1-w{w}") for w in (64, 256, 1024)]),
    "kv-steady": lambda: kv_table(S, [("mix, 1 per call", f"kv-steady-mix-b1-w{w}") for w in (16, 64, 256, 1024)]
                                  + [("mix, 25 per call", f"kv-steady-mix-b25-w{w}") for w in (64, 256)]
                                  + [("`incr` only", f"kv-steady-incr-b1-w{w}") for w in (64, 256)]),
    "kv-paced": lambda: table(PACED_HEAD, [paced_row(f"mix offered at {r:,}/s", S, f"kv-steady-mix-b1-paced-{r}")
                                           for r in (5000, 20000, 40000)]),
    "kv-batch": lambda: kv_table(S, [("mix, 5 per call", "kv-mix-b5-w64")]
                                 + [("mix, 25 per call", f"kv-mix-b25-w{w}") for w in (16, 64, 256, 1024)]
                                 + [("mix, 100 per call", "kv-mix-b100-w64"),
                                    ("`incr`, 100 per call, no expiry", "kv-get-prefill")]),
    "kv-reads": lambda: kv_table(S, [("`get`, 1 per call", f"kv-get-b1-w{w}") for w in (64, 256, 1024)]
                                 + [("`get`, 25 per call", "kv-get-b25-w256"),
                                    ("`get`, 1 per call, from 7 million rows", "kv-big-get-b1-w256")]),
    "kv-shapes": lambda: kv_table(S, [(f"`put` of a {v} value", f"kv-put-{b}B-w256")
                                      for v, b in (("16 B", 16), ("1 KiB", 1024), ("16 KiB", 16384))]
                                  + [("mix over 10 million keys, 7 million there", "kv-big-mix-b1-w256"),
                                     ("mix over 2.7 million expired keys", "kv-expiry-then-mix")]
                                  + [("mix over 1,000 keys", f"kv-mix-b1-hot1k-w{w}") for w in (64, 256, 1024)]
                                  + [("`incr` of ONE key", f"kv-incr-onekey-w{w}") for w in (16, 64, 256)]),
    "lock-cycle": lambda: table(CYCLE_HEAD, [cycle_row("", S, f"lock-cycle-w{w}") for w in (16, 64, 256, 1024)]
                                + [cycle_row("", S, f"lock-cycle-b{b}-w64") for b in (16, 64)]),
    "lock-renew": lambda: kv_table(S, [("renew, 1 lock per call", f"lock-renew-w{w}") for w in (64, 256, 1024)]
                                   + [("renew, 64 locks per call", "lock-renew-b64-w64")]),
    "lock-fleet": lambda: table(PACED_HEAD, [paced_row("10,000 locks held", S, "lock-fleet-10k"),
                                             paced_row("100,000 locks held", S, "lock-fleet-100k")]),
    "lock-contend": lambda: table(CONTEND_HEAD, [contend_row(label, S, tag) for label, tag in [
        ("1 lock, 2 workers, retry at once", "contend-1x2-spin"),
        ("1 lock, 16 workers, retry at once", "contend-1x16-spin"),
        ("1 lock, 64 workers, retry at once", "contend-1x64-spin"),
        ("100 locks, 16 workers each, retry at once", "contend-100x1600-spin"),
        ("1 lock, 8 workers, the SDKs' wait, held 20 ms", "contend-1x8-sdk-hold20"),
        ("100 locks, 8 workers each, the SDKs' wait, held 20 ms", "contend-100x800-sdk-hold20")]]),
    "sem": lambda: table(CONTEND_HEAD, [contend_row(label, S, tag) for label, tag in [
        ("64 semaphores of 8, 4 workers each: never full", "sem-free-8-w256"),
        ("8 semaphores of 64, 32 workers each: never full", "sem-free-64-w256"),
        ("1 semaphore of 1,024, 256 workers: never full", "sem-free-1024-w256"),
        ("1 semaphore of 8, 32 workers, held 2 ms", "sem-full-8-w32"),
        ("1 semaphore of 64, 256 workers, held 2 ms", "sem-full-64-w256")]]),
    "guard": guard_pivot,
    "bounds": lambda: table(BOUND_HEAD, [bound_row(label, S, tag) for label, tag in [
        ("mix, 1 per call", "kv-mix-b1-w16"), ("mix, 1 per call", "kv-mix-b1-w256"),
        ("mix, 1 per call", "kv-mix-b1-w1024"), ("mix, 1 per call", "kv-mix-b1-w4096"),
        ("mix, 25 per call", "kv-mix-b25-w256"), ("mix, 25 per call", "kv-mix-b25-w1024"),
        ("lock pairs, 1 per call", "lock-cycle-w1024"), ("lock pairs, 64 per call", "lock-cycle-b64-w64"),
        ("transaction: 1 push", "txn-push-w1024"), ("transaction: 1 push + guard", "txn-push-guard-w1024"),
        ("transaction: 1 push + KV put", "txn-push-kv-w1024")]]),
    "disk": lambda: table(BOUND_HEAD, [bound_row(label, S, tag) for label, tag in [
        ("mix over 2,000,000 keys, fresh broker", "kv-mix-b1-w256"),
        ("the same, broker kept", "kv-steady-mix-b1-w256"),
        ("the same, broker kept, paced at 5,000/s", "kv-steady-mix-b1-paced-5000"),
        ("mix over 1,000 keys, fresh broker", "kv-mix-b1-hot1k-w256")]]),
}


def main():
    if "--fill" in sys.argv:
        # Rewrite every `<!-- T:name -->...<!-- /T -->` block of the README.
        path = sys.argv[sys.argv.index("--fill") + 1]
        text = open(path).read()

        def sub(m):
            name = m.group(1)
            return f"<!-- T:{name} -->\n{TABLES[name]()}\n<!-- /T -->"

        text = re.sub(r"<!-- T:([a-z0-9-]+) -->.*?<!-- /T -->", sub, text, flags=re.S)
        open(path, "w").write(text)
        return
    for name, make in TABLES.items():
        print(f"\n<!-- {name} -->\n{make()}")


if __name__ == "__main__":
    main()
