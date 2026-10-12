import os, sys, time
# write + fdatasync of one group-commit-sized block, three ways: a growing file, a file already written once (recycled),
# and a growing file with a concurrent second syncing writer on the same filesystem
SZ = int(sys.argv[1]) if len(sys.argv) > 1 else 1536 * 1024
buf = os.urandom(SZ); SECS = 5
def run(path, prefill):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    cap = 6 << 30
    if prefill:
        z = os.urandom(8 << 20); n = 0
        while n < (3 << 30): os.write(fd, z); n += len(z)
        os.fsync(fd); os.lseek(fd, 0, os.SEEK_SET); cap = 3 << 30
    ts = []; done = 0; t_end = time.perf_counter() + SECS
    while time.perf_counter() < t_end and done + SZ <= cap:
        t = time.perf_counter(); os.write(fd, buf); os.fdatasync(fd); ts.append((time.perf_counter() - t) * 1000); done += SZ
    wall = sum(ts) / 1000
    os.close(fd); os.unlink(path)
    ts.sort(); n = len(ts)
    return f"{n / wall:6.0f} syncs/s  {done / wall / 1e6:6.0f} MB/s  ms p50 {ts[n // 2]:.2f} p99 {ts[min(n - 1, int(n * .99))]:.2f}"
print(f"block {SZ / 1024:.0f} KB | growing file : {run('/root/pat-a.bin', False)}")
print(f"block {SZ / 1024:.0f} KB | recycled file: {run('/root/pat-b.bin', True)}")
