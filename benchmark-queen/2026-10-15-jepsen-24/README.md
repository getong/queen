# Jepsen and a rolling upgrade, the build Alice calls 2.4, 2026-10-15 and 16

The broker of branch `worktree-bytes-per-msg` (`ed73dc1cb`): 2.3.0 plus the three fixes of
[`../2026-10-15-bytes-per-msg/`](../2026-10-15-bytes-per-msg/), each behind cluster version 7:

1. `2c8d02759` a push outcome written as runs (outcome version 7);
2. `ae57e97c5` the checkpoint keeps `txns` rows without their hashes, the boot refills them from the
   queue logs;
3. `cb5a04401` an entry record leaves out the hash lists its message records hold
   (`REC_ENTRY_ELIDED`).

One control node and six test nodes, `n1` to `n6`: DigitalOcean droplets in one VPC, Ubuntu 24.04,
the nodes 2 vCPU / 4 GB, the control 4 vCPU / 16 GB. The harness is in git (`test/jepsen` of
`ed73dc1cb`, unchanged). This folder holds what git does not.

## Result

**Jepsen: 257 tests, all valid**, on the first run, with no history checked again and no test run
twice:

- **Main list**, 213 tests (`test/jepsen/matrix-220.txt` unchanged, the list 2.2.0 and 2.3.0 passed),
  on `n1` to `n5`: 213 valid.
- **Failover**, 31 tests (`test/jepsen/matrix-220b2.txt`, a standby cluster's failover, the source
  on `n1` to `n3`, the standby on `n4` to `n6`): 31 valid.
- **Batched pushes**, 13 tests (`test/jepsen/matrix-24-batch.txt`, below): 13 valid. These are the
  only ones that write push outcomes as runs (fix 1); the 244 above exercise fixes 2 and 3.

**Rolling upgrade 2.3.0 -> 2.4: passed** (section below): the cluster version stays 6 while a 2.3.0
member is in the cluster and rises to 7 by itself after the last one; nothing was refused or
poisoned during the mix; after the rise the three new forms are on every node's disk; a full
restart and a `kill -9` of the leader refill the `txns` rows (0 missing) and every duplicate is
still found; 2.3.0 refuses version-7 data cleanly, both on a node's own directory and as a fresh
node joining the upgraded cluster.

## The build

`queen-24`, md5 `e9fb9091e309c794d66f141253d2e30e` (`binaries.md5`): built on Alice's Mac with the
release recipe, `./build.sh --amd64 --registry queen-jepsen24 --tag 2.4-ed73dc1cb` (the Dockerfile
of the tree, linux/amd64, 8 minutes on the warm cache), from the clean worktree at `ed73dc1cb`; the
image (`sha256:fc214f33...`, label revision `ed73dc1cb`) stayed local, nothing was pushed. Its
`/health` still says `"version": "2.3.0"` (`server/server.json` is not bumped): the cluster version
and `kinds` tell the builds apart.

`queen-230`, md5 `61347636f932e64b4765b9015643c1f6`: the broker of the published
`ghcr.io/queen-mq/queen:2.3.0` (amd64 `sha256:967fc40c...`, what stage and prod run), for the
upgrade test.

## Rolling upgrade 2.3.0 -> 2.4

`upgrade/roll24.sh`, run from the control against three brokers on `n1` to `n3` (one droplet each,
the private network, HTTP 6632, raft 7400, `upgrade/node.sh` on each), before the Jepsen lists. Queues
`hist` (192,000 messages written by 2.3.0 in pushes of 50, never consumed until the end, dedup
window 3,600 s), `live` (steady traffic through the whole roll: three `qload` loaders, each through
its own node, 1,500 msg/s each in pushes of 20 with two consumers each, dedup window 600 s) and
`dup` (`upgrade/dup.py`: sets of 2,000 messages in 20 pushes of 100 with fixed `transactionId`s; a
set is pushed once and then again, and every second answer must be `duplicate` naming the offset
the first push got). Dedup index `txns`, the default. A node moves by SIGTERM, a start on the other
binary and a wait for `/health` healthy with lag 0. `upgrade/roll.log` is the run (its first line
prints no md5s: the script looked for the binaries on the control, they were on the nodes; they are
the two above).

| Step | What was checked | Result |
|---|---|---|
| 0 | a fresh 2.4 cluster, before any load | `clusterVersion` 7 and `kinds` 7 on all three |
| 1 | a fresh 2.3.0 cluster | version 6, `kinds` 6; history written, set A queued (2,000 of 2,000) |
| 2 | `n3`, then `n2`, on 2.4; `n1` still on 2.3.0 | version stays 6 (`kinds` 7, 7, 6); set A all duplicates with their offsets, set B queued; `n3`'s queue logs and checkpoint hold only the version-6 forms: 0 elided entry records, 0 outcomes at version 7, every `txns` row whole |
| 3 | `n3` back to 2.3.0 on the directory 2.4 had written | starts and follows (lag 0); set B all duplicates; then forward again |
| 4 | `n1`, the last 2.3.0 member, on 2.4 | version 7 on all three 3 s after its start, by itself |
| 5 | the new forms, 20 s after the rise | per node (`n1` / `n2` / `n3`): push outcomes at version 7: 4,974 / 5,363 / 5,688, at version 1 still 10,086 (all written before the rise); entry records without hash lists: 4,936 / 5,335 / 5,656; checkpoint `txns` rows of 40 bytes or less (no hashes): 5,223 / 5,591 / 5,815 (the rows written before the rise stay whole); sets A, B, C all duplicates |
| 6 | the whole cluster stopped and started on 2.4 | each boot: `txns rows refilled from the queue logs rows=74391 refilled=74391 missing=0 orphaned=0 ms=376` (`n2` 74,219 in 663 ms, `n3` 74,237 in 402 ms); sets A, B, C all duplicates; the history read back whole: 192,000 popped, 0 errors |
| 7 | `kill -9` of the leader (`n2`) under load, a start 5 s later | `rows=80922 refilled=80922 missing=0` in 1,012 ms; sets A, B, C all duplicates; set D queued, then all duplicates |
| 8a | 2.3.0 on `n3`'s own (version-7) directory | refuses to start within 0.2 s (2.4 starts on that directory again afterwards, step 8b): `this build cannot read the log of the cluster this store belongs to: refusing to start node=3 cluster_version=7 reads=6`, then `FATAL: raft storage failed to open: ... this build reads effect catalogue version 6, and the cluster this store belongs to writes version 7 (§12.8): run a release that reads it (the one the other nodes run), never an older one` |
| 8b | 2.3.0 on an EMPTY directory, as `n3`, joining the version-7 cluster | joins as a fresh node that serves 503, and refuses every replicated append (`replicated entry does not decode: UnknownCatalogue(7)`, 244 times in 30 s): nothing applied; `n1` and `n2` healthy throughout; `n3` back on 2.4 with its own directory catches up (lag 0), set D all duplicates |

How the forms were read: `bpm` of [`../2026-10-15-bytes-per-msg/scripts/tool`](../2026-10-15-bytes-per-msg/scripts/tool),
with two counters added for this run (outcomes by tag and version; checkpoint values by size:
40 bytes or less, 41 to 512, more than 512, overflow). Its `BAD: entry body_len 4294967295` lines
in `roll.log` are openraft's own entries (Blank, Membership: magic `u32::MAX`), which the tool does
not decode; not a fault.

Traffic over the roll (step 1 to 5, 360 s, 4,500 msg/s offered): 1,607,380 messages pushed, 1,607,880
popped (the 500 more are redeliveries: 500 acks failed while a node restarted). 404 pushes (8,080
messages) failed, every one a transport error (`connection refused`, `EOF`) of the loader whose own
node was restarting: `qload` talks to one node and never retries. In step 7 the loader of the killed
leader lost 611 pushes the same way, and acks in flight got `503 outcome_unknown` ("the leader
changed while this change was being made durable").

Node logs over all starts: no FATAL, panic, `poisoned` or unknown-version line on 2.4; the one FATAL
is step 8a's 2.3.0. Every WARN is openraft's `Unreachable node`, `the append stream closed` or a
failed heartbeat towards a peer that was restarting.

Not covered: a 2.3.0 node receiving a SNAPSHOT of version-7 data (in step 8b the leader never got
past the first append, so it sent none); five nodes; a roll under the Jepsen nemeses.

## Jepsen

The harness is `test/jepsen` of `ed73dc1cb` for the main and failover lists (`/root/jepsen-24`, heap
`-Xmx12g` on the 16 GB control, `scripts/project.clj.as-run`) and of `703d31fe7` for the batched
pushes (`/root/jepsen-batch`). Every test ran `queen-24`, md5 `e9fb9091`. lazyfs 0.3.0 was built on
all six nodes before the first test, so no power-loss test raced its build (the failover list of
2.3.0 lost a run to that).

| When (UTC) | What |
|---|---|
| 10-15 13:05 to 13:15 | the rolling upgrade 2.3.0 -> 2.4 on `n1` to `n3` (above) |
| 10-15 13:17 to 13:20 | the setup check `s-w8-none`: valid; every `/health` sample during it read cluster version 7 (`setup-check/cv.txt`) |
| 10-15 13:20 | the main list starts (`scripts/go-24.sh`, `scripts/campaign-24.sh`) |
| 10-15 13:30 to 13:35 | the batched-push modes on `n6` alone, beside the main list (`batch-smoke/`): 3 valid |
| 10-16 09:21 | the main list ends: 213 valid |
| 10-16 12:12 | the failover list ends: 31 valid (`scripts/campaign-24-fo.sh`) |
| 10-16 13:32 | the batched-push list ends: 13 valid (`scripts/campaign-24-batch.sh`) |

About 5.6 minutes a test. Stage ran 2.4.0 from 10-16 07:20 and prod from 10:47, both before the
failover and batched lists had ended.

## Batched pushes: fix 1 under Jepsen

Every workload of the 244 tests pushes one message per push (`:items [ ... ]` with one item, also
inside transactions), and a one-message push keeps its version-1 outcome (29 bytes against 37 as
runs). So none of the 244 writes a push outcome at version 7; fixes 2 and 3 apply to every push,
and the kills, restarts and power losses of the suite exercise them.

`--batch-sends N` (commit `703d31fe7`) makes pushes carry several
messages of one partition: the log workload sends an op's 1 to N sends to one key as one push (one
key per push: a plain push is not atomic across partitions, and the Kafka checker judges an op as
one transaction), the dedup workload pushes 2 to N distinct ids of one partition and retries an
unknown answer once on another node. `test/jepsen/matrix-24-batch.txt` has 13 tests (nine log,
four dedup): kills, partitions, pauses, power loss, leader-deaf, graceful restarts, forced
snapshots (`member-snap`), clock jumps and a 2 s rows window. They run after the failover list,
on `n1` to `n5`, into `batch/`.

Smoke first, on `n6` alone (one voter) beside the main list, 2026-10-15 13:30 to 13:35 UTC
(`batch-smoke/`): `bs-w1-none`, `bs-w6-kill`, `bs-w1-kill` all valid. Read on the node's disk right
after each log test: 1,694 of 3,639 push outcomes were runs (version 7) after `bs-w1-none`, 353 of
769 after `bs-w1-kill` (the others one-message pushes, version 1); 3,613 of 3,634 entry records
elided; every `txns` row of the checkpoint without its hashes.

Result, 2026-10-16 12:12 to 13:32 UTC (`batch/`): **13 of 13 valid**: the nine log tests (kill,
partition, pause, power loss, leader-deaf, graceful restart, forced snapshots, clock, rows that live
2 s) and the four dedup tests (kill, partition, restart, power loss).

## What it does not cover

- The same `transactionId` twice inside ONE push: what the broker does is not documented, and the
  batched dedup mode never does it.
- The binary wire forms (binary push, compressed groups, framed pop, the binary transaction body):
  the harness's client speaks JSON. The Kafka facade: no workload uses it.

## Files

| Path | Contents |
|---|---|
| `upgrade/` | `roll24.sh`, `node.sh`, `dup.py`, `health.py`, `roll.log`, the loaders' final numbers |
| `setup-check/`, `main/`, `failover/`, `batch/` | one folder per list: `summary.txt` (one `start` and one `done` line per test), `results/<test>.edn`, `matrix.txt`, `binary.md5` |
| `batch-smoke/` | the batched-push modes on `n6` alone (`forms-n6.txt`: the node's disk after each log test) |
| `scripts/` | the launchers, `collect-jepsen-24.sh` (this folder), `project.clj.as-run` |
| `binaries.md5` | both builds |

The Jepsen stores (every history, the Jepsen logs and each node's `queen.log`) were not copied.
