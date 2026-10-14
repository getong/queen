"""Summary of the 3M matrix: latency + per node CPU, memory, disk (MB/s, ms a write, busy %), network; last 30 s of load.
Writes runs/matrix-gcp24-3m.md and .json."""
import json,re,os,sys
R="/Users/alice/Work/queen/benchmark-queen/2026-10-12-three-node-new-forms/runs/"
PFX=sys.argv[1] if len(sys.argv)>1 else "m3"
OUT=sys.argv[2] if len(sys.argv)>2 else "matrix-gcp24-3m"
HEAD=sys.argv[3] if len(sys.argv)>3 else "3M msg/s"
WHAT=sys.argv[4] if len(sys.argv)>4 else "100 msg a request, 256 B JSON"
rows={}
for l in open(R+"grid-gcp.jsonl"):
    d=json.loads(l)
    if d["dir"].startswith(PFX+"-p"): rows[d["partitions"]]=d
def net(path):
    rs=[]
    for line in open(path):
        m=dict(re.findall(r'(\w+)=([\d.]+)',line))
        if 't' in m: rs.append({k:float(v) for k,v in m.items()})
    out=[]
    for a,b in zip(rs,rs[1:]):
        dt=b['t']-a['t']; w=b['wios']-a['wios']
        out.append(dict(t=b['t'],mb=(b['wsec']-a['wsec'])*512/dt/1e6,ms=(b['wticks']-a['wticks'])/w if w else 0.0,
                        busy=(b['ioticks']-a['ioticks'])/dt/10,rx=(b['rx']-a['rx'])/dt/1e6,tx=(b['tx']-a['tx'])/dt/1e6))
    return out
def sample(path):
    rs=[dict(re.findall(r'(\S+)=(\S+)',l)) for l in open(path)]
    return rs
res=[]
for P in sorted(rows):
    d=rows[P]; base=R+d["dir"]+"/"
    n={i:net(base+f"net-n{i}.log") for i in (1,2,3)}
    # the load: seconds where the leader receives more than half its peak; the last 30 of them
    peak=max(x['rx'] for x in n[1]); load_t=[x['t'] for x in n[1] if x['rx']>peak/2]
    t_end=load_t[-1]; t_beg=t_end-30
    def win(xs): return [x for x in xs if t_beg<x['t']<=t_end]
    disk={i:dict(mb=sum(x['mb'] for x in win(n[i]))/max(1,len(win(n[i]))),
                 ms=sorted(x['ms'] for x in win(n[i]))[len(win(n[i]))//2] if win(n[i]) else 0,
                 ms_max=max((x['ms'] for x in win(n[i])),default=0),
                 busy=sum(x['busy'] for x in win(n[i]))/max(1,len(win(n[i]))),
                 rx=sum(x['rx'] for x in win(n[i]))/max(1,len(win(n[i]))),
                 tx=sum(x['tx'] for x in win(n[i]))/max(1,len(win(n[i])))) for i in (1,2,3)}
    s={i:sample(base+f"sample-n{i}.log") for i in (1,2,3)}
    mem={}
    for i in (1,2,3):
        sw=[x for x in s[i] if t_beg-10<float(x['t'])<=t_end+10] or s[i]
        mem[i]=dict(rss_gb=max(int(x['rss_kb']) for x in sw)/1048576, free_min_gb=min(int(x['memfree_kb']) for x in sw)/1048576)
    # leader per-thread cores over the window (10 s rows, cumulative ticks)
    sw=[x for x in s[1] if t_beg-10<float(x['t'])<=t_end+10]
    th={}
    if len(sw)>=2:
        a,b=sw[0],sw[-1]; dt=float(b['t'])-float(a['t'])
        for k in b:
            if k.startswith('th_'): th[k[3:]]=(int(b[k])-int(a.get(k,0)))/100/dt
    top=sorted(th.items(),key=lambda kv:-kv[1])[:4]
    res.append(dict(partitions=P,carried=d["carried"],offered=d["offered"],pushed=d["pushed"],consumed=d["consumed"],shed=d["shed"],errors=d["errors"],
        push_p50=d["push_p50_ms"],push_p99=d["push_p99_ms"],e2e_p50=d["e2e_p50_ms"],e2e_p99=d["e2e_p99_ms"],win_e99=d.get("win_e99"),
        cores=d["cores"],loader_cores=d["loader_cores"],mem=mem,disk=disk,leader_threads=top,dir=d["dir"]))
json.dump(res,open(R+OUT+".json","w"),indent=1)
L=[f"# {HEAD} by partitions, GCP c4-standard-24-lssd x3 (RAID0 of 4 local NVMe), Queen 2.3.0, 2026-10-14","",
f"Fresh cluster a point, {WHAT}, dedup off, completed kept 1,800 s, binary push + zstd + counts ack + framed pop.",
"Latency: last 30 s of a 70 s step (harness rows). Disk/net: mean of the last 30 s of load from /sys/block/md0 and the private NIC, per node n1 (leader) / n2 / n3; ms a write = median of the per-second means. Memory: broker RSS max and machine free memory min around that window.","",
"| partitions | carried | push p50 / p99 | e2e p50 / p99 | cores n1/n2/n3 | broker RSS GB | free mem min GB | disk MB/s | ms a write (max) | disk busy % | leader rx / tx MB/s | loaders cores |",
"|---|---|---|---|---|---|---|---|---|---|---|---|"]
for r in res:
    m=r["mem"]; k=r["disk"]
    L.append(f'| {r["partitions"]:,} | {"yes" if r["carried"] else "NO"} | {r["push_p50"]:.1f} / {r["push_p99"]:.1f} | {r["e2e_p50"]:.1f} / {r["e2e_p99"]:.1f} | '
             f'{" / ".join(f"{c:.1f}" for c in r["cores"])} | {" / ".join(f"{m[i]["rss_gb"]:.1f}" for i in (1,2,3))} | {" / ".join(f"{m[i]["free_min_gb"]:.0f}" for i in (1,2,3))} | '
             f'{" / ".join(f"{k[i]["mb"]:.0f}" for i in (1,2,3))} | {" / ".join(f"{k[i]["ms"]:.2f} ({k[i]["ms_max"]:.1f})" for i in (1,2,3))} | {" / ".join(f"{k[i]["busy"]:.0f}" for i in (1,2,3))} | '
             f'{k[1]["rx"]:.0f} / {k[1]["tx"]:.0f} | {r["loader_cores"]:.1f} |')
L+=["","Leader threads (cores, last 30 s): "]+[f'- {r["partitions"]:,}: '+", ".join(f"{n} {c:.2f}" for n,c in r["leader_threads"]) for r in res]
L+=["","Per 10 s e2e p99 (ms): "]+[f'- {r["partitions"]:,}: {r["win_e99"]}' for r in res]
L+=["","Raw: runs/<dir>/ (net-n*.log per second: disk + NIC; sample-n*.log per 10 s: memory + CPU per thread; q*.json loaders; broker-n*.log.gz)."]
open(R+OUT+".md","w").write("\n".join(L)+"\n")
print("\n".join(L))
