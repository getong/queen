import json,glob,sys
t=sys.argv[1]; node=sys.argv[2] if len(sys.argv)>2 else "b1"
w=[x for x in json.load(open(f"{t}/loaders/l1/p0.json"))["windows"] if x["offered_per_s"]>0]
wins=[(x["end_ms"]/1000-x["secs"], x["end_ms"]/1000, round(x["p99_ms"])) for x in w]
rows=[]
for l in open(f"{t}/samples/{node}.txt"):
    if l.startswith("#"): continue
    p=l.split(); d={k:v for k,v in (x.split("=",1) for x in p[1:] if "=" in x)}; d["t"]=float(p[0]); rows.append(d)
def f(d,k):
    try: return float(d.get(k,0))
    except: return 0.0
print(f"{t} {node}: per 2 s  | window p99 (process 0)")
print("  t(s)  busy  iow steal | psi_cpu psi_mem psi_io | diskMB/s  flush/s  dirtyMB  wbMB | rxMB/s | pgscan_d allocst")
t0=wins[0][0]
for a,b in zip(rows,rows[1:]):
    if b["t"]<t0-4 or b["t"]>wins[-1][1]+2: continue
    dt=b["t"]-a["t"]; tot=f(b,"cpu_total")-f(a,"cpu_total"); n=f(b,"ncpu") or 1
    busy=(f(b,"cpu_busy")-f(a,"cpu_busy"))/tot*n if tot else 0; iow=(f(b,"cpu_iowait")-f(a,"cpu_iowait"))/tot*n if tot else 0; st=(f(b,"cpu_steal")-f(a,"cpu_steal"))/tot*n if tot else 0
    win=[p for (s,e,p) in wins if s<=b["t"]<e]
    print(f"  {b['t']-t0:5.0f} {busy:5.1f} {iow:4.1f} {st:4.2f} | {b.get('psi_cpu','?'):>6} {b.get('psi_mem','?'):>6} {b.get('psi_io','?'):>6} | {(f(b,'disk_wsect')-f(a,'disk_wsect'))*512/dt/1e6:7.0f} {(f(b,'disk_flush')-f(a,'disk_flush'))/dt:7.1f} {f(b,'Dirty_kb')/1024:7.0f} {f(b,'Writeback_kb')/1024:5.0f} | {(f(b,'net_rx')-f(a,'net_rx'))/dt/1e6:5.0f} | {f(b,'pgscan_direct')-f(a,'pgscan_direct'):6.0f} {f(b,'allocstall')-f(a,'allocstall'):5.0f}   {win[0] if win else ''}")
