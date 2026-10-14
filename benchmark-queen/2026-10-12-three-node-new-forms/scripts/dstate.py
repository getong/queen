# dstate.py <out> <seconds>: waits for the Kafka broker JVM, then every 20 ms lists its threads in D state (blocked in
# the kernel, usually on IO) with their name, wchan and the top of the kernel stack; also jbd2 threads. Every 1 s the
# block counters of md0 and each local NVMe disk. For the 2026-10-14 investigation of Kafka's 1k-partition tail.
import os, sys, time, glob
out, dur = sys.argv[1], float(sys.argv[2])
def kpid():
    for p in os.listdir("/proc"):
        if p.isdigit():
            try:
                if b"kafka.Kafka" in open(f"/proc/{p}/cmdline","rb").read(): return p
            except Exception: pass
pid = None; t0 = time.time()
while not pid and time.time() - t0 < 600: pid = kpid(); time.sleep(0.5)
f = open(out, "w"); f.write(f"# pid {pid} start {time.time():.3f}\n")
devs = ["md0"] + sorted(os.path.basename(x) for x in glob.glob("/sys/block/nvme*n1") if "nvme0n1" != os.path.basename(x) or True)
jbd = [p for p in os.listdir("/proc") if p.isdigit() and open(f"/proc/{p}/comm").read().startswith("jbd2")]
end = time.time() + dur; nxt = 0
while time.time() < end and pid and os.path.exists(f"/proc/{pid}"):
    now = time.time()
    tasks = [(pid, t) for t in os.listdir(f"/proc/{pid}/task")] + [(j, j) for j in jbd]
    for p, t in tasks:
        try:
            st = open(f"/proc/{p}/task/{t}/stat").read()
            state = st[st.rindex(")") + 2]
            if state == "D":
                comm = open(f"/proc/{p}/task/{t}/comm").read().strip()
                wchan = open(f"/proc/{p}/task/{t}/wchan").read().strip()
                try: stack = "|".join(l.split()[1].split("+")[0] for l in open(f"/proc/{p}/task/{t}/stack").read().splitlines()[:8])
                except Exception: stack = ""
                f.write(f"D {now:.3f} {t} {comm.replace(' ','_')} {wchan} {stack}\n")
        except Exception: pass
    if now >= nxt:
        nxt = now + 1
        for d in devs:
            try: f.write(f"S {now:.3f} {d} {open(f'/sys/block/{d}/stat').read().split()}\n".replace("'","").replace(",",""))
            except Exception: pass
    time.sleep(max(0, 0.02 - (time.time() - now)))
f.close()
