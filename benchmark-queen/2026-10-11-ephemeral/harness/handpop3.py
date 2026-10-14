# A pop already waiting on an EMPTY durable partition, one push 0.5 s later: when does the pop answer?
import json, sys, threading, time, urllib.request
B = sys.argv[1]; H = {"x-queen-tenant": "00000000-0000-4000-8000-000000000000", "content-type": "application/json"}
def call(method, path, body=None, timeout=10):
    req = urllib.request.Request(B + path, data=json.dumps(body).encode() if body is not None else None, headers=H, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(); return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, None
def ack(msgs, group):
    for m in msgs:
        if m.get("leaseId"):
            call("POST", "/api/v1/ack", {"transactionId": m["transactionId"], "partitionId": m["partitionId"], "status": "completed",
                                         "error": None, "consumerGroup": group or "__QUEUE_MODE__", "leaseId": m["leaseId"]})
def run(label, queue, part, pinned, commit, group):
    base = f"/api/v1/pop/queue/{queue}" + (f"/partition/{part}" if pinned else "") + "?batch=10" + ("&commitOnDelivery=true" if commit else "") + (f"&consumerGroup={group}" if group else "")
    out = []
    for k in range(5):
        for _ in range(5):                      # empty it first
            s, b = call("GET", base)
            if s != 200 or not b or not b.get("messages"): break
            ack(b["messages"], group)
        res = {}
        def waiter():
            s, b = call("GET", base + "&wait=true&timeout=1500")
            res["end"], res["status"], res["msgs"] = time.time(), s, (b or {}).get("messages", [])
        th = threading.Thread(target=waiter); th.start()
        time.sleep(0.5); t0 = time.time()
        call("POST", "/api/v1/push", {"items": [{"queue": queue, "partition": part, "payload": {"n": k}}]})
        th.join()
        out.append(f"{(res['end'] - t0) * 1000:5.0f} ms [{res['status']}, {len(res['msgs'])} msg]")
        ack(res["msgs"], group)
    print(f"{label:46s} " + " | ".join(out), flush=True)
run("one partition, commit at delivery, no group", "h3a", "p", True, True, None)
run("one partition, lease + ack, no group", "h3b", "p", True, False, None)
run("one partition, lease + ack, group g", "h3c", "p", True, False, "g")
run("any partition, lease + ack, no group", "h3d", "p", False, False, None)
run("any partition, lease + ack, group g", "h3e", "p", False, False, "g")
