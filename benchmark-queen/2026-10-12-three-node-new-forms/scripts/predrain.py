#!/usr/bin/env python3
"""predrain.py <dir>: one line on mrun.sh's consumers-only phase: what it consumed of the prefill and whether it was done."""
import glob, json, sys

js = [json.load(open(f)) for f in glob.glob(sys.argv[1] + "/q*.json")]
last = sum(w["popped_per_s"] for j in js for w in j["windows"][-3:-1]) / 2 if js else -1
errs = sum(j["pop_errors"] + j["ack_errors"] for j in js)
warm = sum(j.get("warm_received", 0) for j in js)
print(f"{len(js)} processes consumed {warm:,} prefill messages, {last:,.0f}/s in the last windows, errors {errs}")
