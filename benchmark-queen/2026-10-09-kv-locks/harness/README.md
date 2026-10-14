# Harness

Everything here runs on the VM, from `/root/kvl`.

| File | What it does |
|---|---|
| `broker.sh <single\|cluster3> <start\|stop\|wipe\|status>` | Starts the broker from `bin/queen`: one node, or three voters on this machine. Code defaults, bound to 127.0.0.1, KV rate limits raised (`DEFAULT_RATES=1` keeps them). `DATA_ROOT=/dev/shm/kvl` puts the data in RAM |
| `run.sh <topology> <tag> <kvlockload flags>` | One measured point: a fresh broker (`KEEP=1` keeps the running one and its data), the loader, and the broker's counters over the measured window, in `runs/<topology>/<tag>/` |
| `matrix.sh <phase>` | The points of one phase, in order; log in `runs/matrix-<phase>.log`, marker `runs/DONE-<phase>` |
| `snap.py take\|diff` | Per broker process: CPU by thread name, `/metrics/prometheus`, RSS; for the host: CPU and the disk's writes and flushes. The loader calls `take` at both edges of the measured window (`-snap`), `run.sh` writes the `diff` to `broker.txt` |
| `watch-expiry.sh`, `count-keys.sh` | Every 5 s: the expiry gauges, rows deleted, RSS, data directory size; and the KV rows a node holds, counted through the namespaces route |
| `chain.sh <phase> <phase>...` | Waits for one phase's `DONE` marker, then runs the next ones  |
| `report.py [runs] [--fill README.md]` | Fills the tables of `../README.md` from the runs |
| `table.py <runs>` | One table row per point, from each run's `load.txt` and `broker.txt` |

Set-up from a Mac:

```bash
# the released binary, out of the release image
docker pull --platform linux/amd64 ghcr.io/queen-mq/queen:2.1.0
docker cp "$(docker create --platform linux/amd64 ghcr.io/queen-mq/queen:2.1.0)":/app/bin/queen queen
# the loader
(cd ../kvlockload && GOWORK=off GOOS=linux GOARCH=amd64 go build -o kvlockload-linux-amd64 .)
ssh root@VM 'mkdir -p /root/kvl/bin'
scp queen root@VM:/root/kvl/bin/queen
scp ../kvlockload/kvlockload-linux-amd64 root@VM:/root/kvl/bin/kvlockload
scp broker.sh run.sh matrix.sh snap.py watch-expiry.sh table.py root@VM:/root/kvl/
ssh root@VM 'cd /root/kvl && nohup ./matrix.sh kv-sweep >/dev/null 2>&1 &'
```

On 2026-10-09 the phases `kv-sweep`, `kv-shapes`, `locks`, `contend`, `guard`, `kv-expiry`,
`kv-steady` and `kv-extra` ran, in that order. `cluster`, `kv-tmpfs`, `extras` and `kv-headline`
are written and did not run.

Three traps met on 2026-10-09:

- **Never copy over `matrix.sh` while a phase runs.** `scp` rewrites the file in place and bash
  reads a script as it goes: the running phase went on into the new file's text, ran five points
  nobody asked for and died on a syntax error before its `DONE` marker. Copy to another name and
  `mv` it over (the running shell keeps the old file).
- **`pkill -f chain.sh` over ssh kills the ssh command itself**, whose own command line holds
  the pattern. Write `pkill -f "[c]hain\.sh"`.
- **A point on a fresh broker flatters KV writes.** The first seconds on an empty store run about
  a third faster than the minute that follows. `kv-steady` warms one broker and keeps it.
