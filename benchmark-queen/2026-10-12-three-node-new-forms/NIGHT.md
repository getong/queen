# The night of 2026-10-12: three fixes, tactical runs, final matrices

Working log of the run that started at 19:12 UTC. Same six machines as the rest of this folder (brokers `queen`,
`queen-02`, `queen-03`; loaders `queen-04` to `queen-06`), same harness (`scripts/`). Nothing here is committed.

## What was wrong

| Symptom | Cause | Where |
|---|---|---|
| From about second 70 of every load the end to end p99 doubles or triples | Every command's outcome is kept 60 s (`QUEEN_RAFT_REQUEST_ID_WINDOW_S`) and the expiry step ran on a 10 s tick: ten seconds of outcomes left together, in back-to-back steps of 4,096 rows, and the apply thread did little else meanwhile (stalls of 50 to 150 ms) | `rsm/batcher.rs`, `request_expire_every_ms` |
| With 10M partitions 7% of messages are up to 80 ms late at any load | The consume engine's idle-part scan took a whole shard (625,000 parts) under its lock once a second, on the thread that serves waiting pops | `rsm/consume/unload.rs` |
| At low load a pop's answer waits 2.5 ms on average before its replicated round begins | The checkpoint that carries its lease is taken on a 5 ms tick | `rsm/consume/checkpoint.rs`, `rsm/facade/intake.rs` |

Probe behind the first row: `runs/t0-probe23-p1000-r1500000/1500000/probe` (the apply thread never blocks; from second
70 it runs `RamTable::remove`, and calls `madvise` five times as often).

## What changed (binary `p1`)

1. `unload.rs`: the scan works in turns of 1,024 partitions by partition id, one turn every 5 ms, a pass at most every
   16 s. A turn holds its shard about 0.1 ms.
2. Eager checkpoint (`QUEEN_CONSUME_CHECKPOINT_EAGER`, on): an answer that begins to wait wakes the ticker at once
   when no checkpoint is in flight; when one is, the ticker is woken as the last one commits. The 5 ms tick stays as
   the upper bound. No answer is sent earlier than its row is durable.
3. `QUEEN_RAFT_REQUEST_EXPIRE_FAST_MS` (10): every 10 ms the batcher looks whether the oldest outcome left the window
   and, if so, runs the expiry step in the next cycle. What a step retires is unchanged (the 4,096 limit is a
   constant of the state machine and was not touched).

## Tactical runs, 1,000 partitions, dedup off, nothing deleted, 90 s

End to end p50 / p99 in ms, last 30 s of 90, all processes pooled.

| Load | 2.3.0 before (`23`) | `p1` | Brokers' cores, before → `p1` |
|---|---|---|---|
| 100k | 6.5 / 10.2 | 3.9 / 6.8 | 3.1 → 4.4 |
| 500k | 7.9 / 18.7 | 6.0 / 10.4 | 10.2 → 11.0 |
| 1M | 11.8 / 29.9 | 10.6 / 16.3 | 13.2 → 13.5 |
| 1.5M | 17.4 / 78.8 | 17.4 / 32.8 | 16.4 → 16.4 |
| 2M | 29.4 / 127.0 | 30.0 / 60.4 | 20.0 → 20.0 |

With the eager checkpoint off, `p1` at 100k reads 6.6 / 11.0 and 3.1 cores: the eager checkpoint is what moves the
low loads, at the price of twice the raft entries there (1,200 → 2,300 a second at 100k).

10M partitions at 100k msg/s: 6.8 / 73.7 before, 4.0 / 6.9 with `p1` (the same as 1,000 partitions).

## Second round of findings (20:00 to 20:45 UTC)

| Symptom | Cause | Fix |
|---|---|---|
| With retention on, the end to end p99 doubles from second 70 (1,000 partitions, 1M msg/s: 17 → 36 ms; the leader's apply stalls 44 to 59 ms every 5 s) | The retention scanner walks a small store in one slice, once an interval: every partition's watermark reaches the planner together and one entry retires five seconds of rows (50,000 at 1M msg/s) | `rsm/retention_scan.rs` `spread`: a round over a small store is spread over the interval in 100 steps (binary `p2`) |
| 10M partitions at 1.5M msg/s: every client stalls 0.7 s once the three nodes run out of free memory | 66 MB of kernel reserve; the background reclaim started 31 MB above it and was outrun at 200 MB/s | `os-tune.sh`: `vm.min_free_kbytes` 512 MB, `vm.watermark_scale_factor` 200, on the brokers |
| 10M partitions: the leader holds 8 GB more than a follower | A consumer group's first contact loads a part for every partition (about 850 B each with its table), and an idle one stays 600 s | `QUEEN_CONSUME_IDLE_UNLOAD_S=30` for the 10M runs (a setting, not a code change): 20.7 → 14.4 GB at 1M msg/s |

Tactical runs behind them (`runs/t*`, summaries in `runs/tacq.log`):

| Run | Point | e2e p50 / p99, last 30 s | Note |
|---|---|---|---|
| `t8` | 10M partitions, 1M msg/s, `p1` | 11.1 / 17.2 | was 13.4 / 130.1; leader 20.7 GB |
| `t9` | same, idle unload 30 s | 11.3 / 17.7 | leader 14.4 GB; one window at 38 ms |
| `t10` | 10M partitions, 1.5M msg/s, idle unload 30 s | 20.2 / 409.6 | carried for the first time; one 0.7 s stall at second 65 when free memory ran out on all three nodes |
| `t11` | 1,000 partitions, 1M msg/s, dedup on, retention 60 s, 150 s, `p1` | 11.4 / 35.8 | p99 per 10 s: 16-18 until second 70, 28-43 after |

Single-message requests, first look (`p1`, 1,000 partitions, dedup and retention on, 60 s steps on one cluster):
50,000 req/s: push 4.3 / 11.1 ms, e2e 9.6 / 24.6 ms, 19.5 broker cores. 100,000 req/s: push 9.5 / 28.2, e2e 32 / 74,
25.4 cores. 200,000 req/s: not carried (145,000 pushed). At 100,000 req/s no thread of the leader is out of CPU
(10.3 cores: HTTP workers 5.5, apply 1.4 over four threads, raft 1.3, planner 0.3); the planner runs 960 cycles a
second of about 106 commands each, a commit takes 4 ms, and an entry waits 7 to 13 ms to be applied.
Profile: `/root/prof/c3probe` on `queen`.

## Third round (20:50 to 22:00 UTC): binaries `p2` and `p3`, the settings that were kept

`p2` = `p1` + the retention round spread over its interval + the unloader's tables shrunk with hysteresis.
`p3` = `p2` + the parts boxed in the consume engine's tables (`consume/state.rs`): the table holds a pointer per
partition, so growing it no longer moves 430-byte parts under the shard's lock (16 shards doubling in the same
second gave 107 and 163 ms spikes at 10M partitions and 1.5M msg/s) and a free slot costs 16 bytes, not 430.
Unit suite on the `p3` code: 1323 + 1319 passed, 0 failed.

| Run | Point | e2e p50 / p99, last 30 s | Note |
|---|---|---|---|
| `t12` | 1,000 partitions, 1M msg/s, dedup on, retention 60 s, 150 s, `p2` | 11.8 / 19.5 | flat for 150 s (`p1`: 11.4 / 35.8); the realistic condition now reads like the clean one |
| `t18` | same at 2M msg/s, 240 s, `p3` | 43.0 / 78.8 | carried for four minutes, p99 60 to 100 ms per window |
| `t15` | 10M partitions, 1.5M msg/s, idle unload 30 s, `p2`, kernel reserve raised | 19.7 / 85.0 | carried; two windows at 107 and 163 ms (the tables growing) |
| `t17` | same on `p3` | 20.0 / 130.0 | seconds 30-60: 18.8 / 60; the end of the run is tight on memory (leader 17.6 GB, nothing is ever deleted in this condition) |
| `t16` | 10M partitions, 2M msg/s, idle unload 30 s, `p2` | not carried | 2.0M for 60 s (36.6 / 239 ms at seconds 30-60), then the followers' disks fall behind as memory runs out (fsync 80 ms on `queen-03`) |

Single-message requests (1,000 partitions, dedup and retention on, 90 s steps, `p3`):

| req/s | push p50 / p99 | e2e p50 / p99 | broker cores | pops/s |
|---|---|---|---|---|
| 25,000 | 2.8 / 5.2 | 5.7 / 12.2 | 16.1 | 9,300 |
| 50,000 | 4.5 / 12.9 | 10.6 / 28.2 | 20.0 | 10,900 |
| 75,000 | 8.0 / 21.8 | 30.2 / 61.4 | 24.0 | 11,900 |
| 100,000 | 11.9 / 27.6 | 124 / 174 | 25.3 | 8,800 |

Without dedup and deletes the hard limit is about 145,000 req/s (`t14`). The limit is the apply stage: a
single-message request is one command, and every command writes its own rows (message row, outcome, counters) and
the consumers' pops come at 9,000 to 12,000 a second with ten partitions each. Settings tried at 100,000 req/s and
found worse than the defaults: `QUEEN_RAFT_APPLY_SHARDS=8` (e2e p50 182 ms), `=1` (not carried),
`QUEEN_RAFT_APPLY_SHARD_MIN=512` (242 ms); the eager checkpoint off changes nothing there (`t23`).

Settings of the final matrices: binary `p3`, brokers at code defaults, kernel reserve raised on the brokers
(`os-tune.sh`), and `QUEEN_CONSUME_IDLE_UNLOAD_S=30` for the 10M-partition columns only.

## Not done tonight, with what each would buy

| Change | Evidence | Why not tonight |
|---|---|---|
| A push's recorded outcome as a run (partition, first offset, count, time) instead of 25 bytes per message | 2.5 KB per 100-message push, kept 60 s: about 55% of the per-push memory, and `Outcome::encode_body` plus the row writes are 10% of apply | The outcome's bytes are replicated state: it needs a cluster-version gate |
| A smaller part in the consume engine (the lease and the rare fields behind a pointer), or no part at all for a partition with nothing to deliver | About 430 bytes a part; 10M partitions: 4.3 GB on the leader for parts that are copies of their cursor row | About 110 call sites |
| `QUEEN_CONSUME_IDLE_UNLOAD_S` default 600 → 30 or 60 | With the scan in slices and the parts boxed, unloading costs nothing visible: 10M partitions at 1M msg/s read 11.3 / 17.7 ms with 30 s against 11.1 / 17.2 with 600 s, with 6 GB less on the leader | A default of yours |
| The store's rows per partition on a follower | 9.5 GB at rest for 10M partitions (950 bytes each): what bounds 10M at 2M msg/s on 31 GB machines | Deep in the store |
| Single-message requests: the per-command rows (outcome, its expiry index) written and deleted one by one on the apply thread; a pop that waits for a few messages before it answers | The limit is apply at about 100,000 commands a second with 9,000 to 12,000 pops a second of ten partitions each | Design work |
| The same planner limit as before for 100-message requests | 2.5M msg/s is pushed at 1,000 partitions with p99 198 ms; one planning cycle at a time | Queue shards |

Two lines for `webdoc/src/content/docs/reference/configuration.mdx` (not added: the page has another session's
uncommitted edits): `QUEEN_CONSUME_CHECKPOINT_EAGER` (on; `0` waits for the tick) next to
`QUEEN_CONSUME_CHECKPOINT_MS`, and `QUEEN_RAFT_REQUEST_EXPIRE_FAST_MS` (10; `0` leaves the expiry to its 10 s tick)
next to `QUEEN_RAFT_REQUEST_ID_WINDOW_S`.

## 2026-10-13, 04:15 UTC: the network cut, and one more fix

The runs are driven from the Mac over ssh. The home network went down at 23:09 UTC: every run after that failed
("the cluster did not become healthy"). Valid before the cut: 27 points of the first final matrix. The realistic
line and the single-message ramps did not run at all and were started again at 04:17 UTC.

`p4` = `p3` + `qlog/dirx.rs`: the cross-file directory's run files reach the device 8 MB at a time while they are
written, instead of in one sync at the end. Found from one point of the first matrix (100,000 partitions at 1.5M
msg/s, `runs/fin-p100000-r1500000-p3`): at second 46 every broker logged a "qlog directory pass" of 116 to 123 ms,
the leader's longest commit went from 15 to 64 ms, and the end to end p99 of that 10 s window from 35 to 138 ms on
all nine load processes. The pass merges the directories of the queue-log files sealed since the last one, about
every 40 s on every node; while nothing is deleted the merged files keep growing. Unit suite on the `p4` code:
1324 + 1320 passed. From 04:58 UTC the remaining points run on `p4`; the 33 points measured before are `p3`.

## 2026-10-13, 05:25 UTC: the high window of the 2M points with a million partitions or more

Alice stopped the realistic matrix to have this fixed first. The re-run of 100,000 partitions at 1.5M msg/s on `p4`
has no high window any more (p99 per 10 s: 26, 39, 38, 31, 35, 34, 32; `p3`: 138 in the fifth). What was left: with
1M and 10M partitions, one 10 s window at 119 to 151 ms at 2M msg/s (seconds 50-60) and at 71 to 92 ms at 1.5M
(seconds 60-70), on `p3` and `p4` alike, on all nine load processes, while the leader's commit, flush and apply
maxima of that window are as in every other one.

The windows fall where the count of partitions written since the start passes 917,504 (7/8 of 2^20): the loaders
write the partitions round robin, one per request, so that is second 50.9 at 2M msg/s and second 66.2 at 1.5M; and
458,752 gives a smaller one (second 27.9 at 2M). Checked with the loaders reporting every second and the leader
profiled in two 5.5 s windows (`scripts/sp.sh`, `rehashprobe.sh`, `stackscan.py`; run `runs/sp1-p4-p1000000-r2000000`):

| second | pushed that second | push p99 | on the planner thread |
|---|---|---|---|
| 27-28 | 1.907M, then 2.084M | 61-63 ms | 36 ms without a break in one `reserve_rehash`, from second 27.50 |
| 50-51 | 1.790M, then 2.221M | 131 ms | 89 ms without a break in one `reserve_rehash`, from second 50.43 |

The table is the router's kept lookup "partition name → committed pid" (`batcher_lanes.rs`, `RouterCycle.pids`,
kept between cycles as `KeptFacts`): one hash table per queue, keyed by the name as a `String`. When it doubles it
hashes every name again, on the serial planner thread, and every push waits. It is the only table growth that held
any thread for 4 ms or more in either window. Two more costs of the same table, not measured: when it passes
`ROUTER_KEEP_MAX_PIDS` (1,048,576 names: second 57 at 2M msg/s with 10M partitions), or when any catalog change is
in flight (a partition or a queue created), the whole table was dropped on the planner thread.

`p5` = `p4` + `batcher_lanes.rs`: the names of a queue live in 256 tables once there are 4,096 of them (`Names`),
so one growth hashes about 3,600 names; and lookups that are dropped go to the drop thread when they hold 4,096
names or more (`spend_lookups`). Tests: `names_fill_many_small_tables_and_lose_nothing`,
`the_router_answers_the_same_once_a_queue_s_names_fill_many_tables`. The same shape one level up (a tenant's queues
in one table: `RouterCycle.queues` and the middle level of `pids`) is unchanged: it would show with hundreds of
thousands of queues in one tenant, which nothing has measured.

The realistic point that had just finished when the matrix was stopped is kept (`runs/rl-p1000-r2000000`, 360 s at
2M msg/s, 1,000 partitions, `p4`): carried, last 60 s 50.7 / 153.6 ms, and it drifts (e2e p50 30 → 56 ms, p99 75 →
105 ms from second 100 to second 350; one apply stall of 304 ms near second 300).

## 2026-10-13, 06:30 UTC: `p5` checked, and the 2M load over six minutes

`p5` on the points that had the high window (e2e p99 per 10 s, ms):

| point | before | `p5` |
|---|---|---|
| 1M partitions, 1.5M msg/s | 23, 34, 41, 47, 29, 28, 71 | 24, 28, 31, 33, 32, 28, 27 |
| 1M partitions, 2M msg/s | 44, 63, 84, 55, 59, 151, 65 | 35, 58, 57, 54, 53, 63, 55 |
| 10M partitions, 1.5M msg/s | 27, 36, 37, 39, 32, 32, 92 | 25, 41, 37, 34, 32, 34, 32 |
| 10M partitions, 2M msg/s | 56, 66, 69, 57, 56, 125, 63 | 72, 88, 71, 69, 61, 69, 67 |

Second by second at 1M partitions and 2M msg/s (`runs/sp2-p5-p1000000-r2000000`): no second above 76 ms end to end
(161 before), the lowest second pushes 1.957M (1.790M before). 100 partitions at 100k msg/s was run again as well
(its first run had one window at 18 ms: a single 80 ms flush of the leader's disk, nothing of the broker's): 5 ms in
every window. The earlier runs keep their directories with the binary's name (`-p3`, `-p4`).

**2M msg/s is not level over six minutes, in any condition.** Three runs of 360 s at 1,000 partitions, e2e p50
per 10 s, first full window → last:

| run | condition | e2e p50 | last 30 s p50 / p99 |
|---|---|---|---|
| `rl-p1000-r2000000` (`p4`) | realistic: dedup 60 s, deletes after 60 s | 31 → 56 ms | 50.7 / 153.6 (last 60 s) |
| `dgA-p5-p1000-r2000000` | nothing deleted from the queue logs, no dedup, rows window 60 s | 27 → 40 ms | 39.4 / 89.1 |
| `dgB-p1000-r2000000` | realistic, the brokers' disk remounted without online discard | 29 → 51 ms | 49.2 / 96.3 |

What moves: the mean flush of an entry on all three nodes (leader 3.7 → 5.9 ms in run A, 4.1 → 6.0 in the
realistic one) while the entries get fewer and larger (8,300 → 5,600 per 10 s); entries times flush time stays at
about 30 s per 10 s, so the log's flush is the stage that is full. No thread is out of CPU (apply 60 → 76% of a
core), the disk rate is level (553 MB/s per broker), memory is level, the device's own flush and discard commands
are cheap (0.09 and 0.07 ms each over the machine's life). So: not the deletes, not deduplication, not online
discard (the mount option was put back after run B). Not known yet: whether the device's write latency itself
rises under 550 MB/s sustained. `netdisk.sh` now samples the device's counters (write requests and their ms,
busy time, flushes, discards) and `scripts/disklat.py` prints them for a step; the realistic points from 06:27 UTC
on carry them. Two more points were added after the 2M line: the realistic condition at 1.5M and at 1M on 1,000
partitions, 360 s each, to see which load is level.

## 2026-10-13, 07:00 UTC: the realistic condition at 2M msg/s does not hold from 100,000 partitions

The realistic line (dedup 60 s, completed messages deleted after 60 s, rows window 60 s; 360 s a point, judged on
the last 60 s) on `p5`:

| partitions | load | setting | result | e2e p50 / p99, last 60 s | run |
|---|---|---|---|---|---|
| 1,000 | 2M | default | carried, rising slowly | 50.7 / 153.6 | `rl-p1000-r2000000` (`p4`) |
| 100,000 | 2M | default | NOT carried: 537,500 shed, p50 59 ms at second 70 → 2.3 s | 2,064 / 2,490 | `rl-p100000-r2000000` |
| 1,000,000 | 2M | default | NOT carried: 4.7M shed | 2,261 / 2,785 | `rl-p1000000-r2000000` |
| 100,000 | 1.5M | default | carried, level from second 130 (p50 32 ms) | 33.8 / 69.6 | `rl-p100000-r1500000` |
| 100,000 | 2M | `RETENTION_INTERVAL=30000` | carried, rising (p50 29 → 60 ms) | 59.4 / 127.0 | `rli30-p100000-r2000000` |

Cause: a retention round visits every partition every 5 s (`RETENTION_INTERVAL`) and proposes a watermark for each
one that has anything past its window. With 100,000 partitions each receiving a batch every 5 s, that is every
partition every round: 100,000 proposals a round, 20,000 watermarks a second next to 20,000 pushes; with 1M
partitions about 400,000 per 20 s round (one per batch written, the same 20,000 a second). The leader's log:
`retention scan round partitions=100000 proposals=100000`; apply's mean wait goes 2.4 → 28 → 128 → 280 ms and the
planning cycles fall from 680 to 188 a second. Each watermark rewrites its partition's row and deletes its rows:
the brokers' disks go from 13,000 write requests a second of 30 KB to 40,000 of 12 KB with 40 in flight
(`scripts/disklat.py`). At 1,000 partitions there are 200 watermarks a second and none of this shows.

Not from the night's changes: the grid of 10-12 on the old binary has 100,000 partitions at 2M msg/s "not carried"
with the same shape (e2e p50 408 → 1,122 ms at seconds 70-90 of a 90 s step, `runs/grid-p100000/2000000`), and
1M partitions ending at 142 ms and rising. A 90 s step showed only the start of it.

One more thing the 1M-partition run shows: the round 60 s after the partitions were created proposes a watermark
for all 1,000,000 at once (`proposals=1000000`), and for those 20 s the e2e p50 is 0.7 to 2.5 s. A benchmark's
doing (a million partitions created in the same half minute), but it is what a mass creation does a minute later.

So the realistic line is measured at 1.5M msg/s with default settings (the heaviest load that holds), and 2M with
defaults stops at 1M partitions. A proposal, not built: let the scanner leave a partition whose expired part is
younger than a slack (a share of its window, capped at tens of seconds), so a watermark covers several rounds'
worth; nothing is deleted earlier than today, only later. The 30 s round shows what that buys.

## 2026-10-13, 07:20 UTC: retention, at Alice's word ("fix retention, make 1, then tactical tests")

The realistic line stopped at four points of seven (1.5M msg/s, `p5`, last 60 s of 360): 1,000 partitions 20 / 34
ms, 10,000 27 / 53, 100,000 34 / 70, 1M 24 / 147; clean at the same load 17-18 / 34-36.

**Option 1, a planning cycle takes a share of the queued watermarks instead of all of them (`p6`): tried, worse,
taken out.** 200 s points, seconds 140-200: 1M partitions p50 23 → 33 ms, p99 120 → 236, apply's longest entry
85-152 → 184-236 ms; 100,000 partitions 32 / 65 → 35 / 71. Same watermarks per round. My reading of the stalls
(one entry's batch of hundreds of watermarks) was wrong.

**What the stalls are** (`runs/pr5-p1000000-r1500000/1500000/probe`: perf of every thread and the apply threads'
waits for 8 s at second 150, `scripts/applyprobe.sh`, `offcpu2.bt`, `threadprof.py`): no apply thread is out of
CPU (the apply thread 42% of a core, its three shard workers 13% each). 26% of the apply thread's samples are in
`LocalMetrics::record_retention`, the dashboard's node-local retention journal: for every watermark it encoded a
JSON record, opened `local.db`, made three writes and closed it, and every 8,192 records it rewrote the file with
two fsyncs — on the apply thread. In wall time the thread stayed in there for stretches of 6 ms (median), 26 ms
(p90), 37 ms (longest), 88 of them in 8 s, bunched (four within 120 ms) while a slice's watermarks come through.

`p7` = `p5` + `local_metrics.rs`: a retention step is kept in RAM (the dashboard reads it from there at once);
the file is written by `LocalMetrics::persist`, which the metrics collector calls every interval (60 s) and the
store when dropped, with the state lock held for the copy alone. What changes for a user: after a crash the last
minute of retention steps is missing from the dashboard's history (the module's contract already: "losing its
unflushed tail may lose a graph point, never queue state"). Test:
`retention_steps_are_written_by_the_flush_and_never_by_the_step`.

## 2026-10-13, 08:20 UTC: `p7` measured, and what is left in retention

Tactical tests, realistic condition, 200 s a point, seconds 140-200, e2e p50 / p99 (median window), `p5` → `p7`:

| point | `p5` | `p7` | clean (30-60 s) |
|---|---|---|---|
| 1M partitions, 1.5M msg/s | 23 / 120 | 21 / 39 | 17 / 34 |
| 100,000 partitions, 1.5M msg/s | 32 / 65 | 28 / 52 | 18 / 36 |
| 100,000 partitions, 2M msg/s | not carried (2 s, 537,500 shed) | 63 / 106, carried | 30 / 56 |
| 1,000 partitions, 1.5M msg/s | 20 / 34 | 19 / 36 | 17 / 35 |

The leader's longest apply per 10 s at 1M partitions: 85-152 ms → 13-20 ms; the burst a minute after a million
partitions are created (a watermark for each in one round): p50 89 → 21 ms. Unit suite on the `p7` code:
1327 + 1323 passed, 0 build warnings. The realistic 1.5M line on `p7` (pooled last 60 s of 200 s): 1,000
partitions 19.2 / 35.8, 10,000 26.6 / 47.6, 100,000 27.6 / 56.8 (profiled re-run 28.9 / 51.2), 1M 20.7 / 39.9;
10, 100 and 10M running.

**Left: the store's checkpoint cannot keep its 250 ms pace at 10,000 to 100,000 partitions with retention on.**
Durable points per 10 s on the leader (and the followers alike), from the `rsm timing` lines:

| run (realistic 1.5M) | durable points per 10 s |
|---|---|
| 1,000 partitions | 40 throughout |
| 10,000 partitions | 40 until second 120, then 12 |
| 100,000 partitions | 40 until second 100, then 2 |
| 1M partitions | 40-45 throughout |
| any clean run | 40 |

The checkpoint thread is always writing (`write_cut_inner`: LMDB puts 26%, its page flush 27%, the stepped
`sync_file_range` 27%; perf of that thread at 10,000 partitions). The brokers' disks at 100,000 partitions take
42,000 write requests a second of 12 KB with 40 in flight and 1.0 ms each, against 5,900 of 76 KB, 1.5 in flight
and 0.22 ms at 1M partitions (`scripts/disklat.py`). Reading: with a push and a watermark per partition every few
seconds, every changed row is in a different LMDB leaf page, so a checkpoint rewrites about a page per row until
its interval grows to the time between two visits of a partition (0.7 s at 10,000 partitions, 6.7 s at 100,000),
where it settles. At 1M partitions the loaders' round robin makes the changes of 250 ms neighbours in key order,
and at 1,000 the same thousand pages take every change. This is what lifts the realistic line at 10,000 and
100,000 partitions (p99 48 and 57 ms against 36 and 40), and the single apply stalls of 60-100 ms seen once or
twice in 100 s there (the old binary had them too: 121, 94, 73 ms) come with it; which step of a late point
costs apply those milliseconds is NOT found (the profiled run had none in its 95 s). Not fixed: it is the shape
of the store's durable form (rows keyed by partition), not a setting.

Scripts added today: `applyprobe.sh` / `applyprobe2.sh` (perf + `offcpu2.bt` / `offcpu3.bt` on the apply threads),
`threadprof.py`, `retcmp.py`, `disklat.py`, `secline.py`, `stackscan.py`, `sp.sh`, `rehashprobe.sh`.

## 2026-10-13, 08:52 UTC: the realistic line at 1.5M msg/s on `p7`

Dedup 60 s, completed messages deleted after 60 s, rows window 60 s; 200 s a point, pooled last 60 s; rows in
`runs/tactical-ret.jsonl`, runs `t7-p<P>-r1500000` (100,000 partitions three times: `t7-`, `pr7-` profiled, `t7b-`).

| partitions | carried | e2e p50 / p99 | clean, same load | cores | leader GB |
|---|---|---|---|---|---|
| 10 | NO: 1.32M pushed, 3.7 s | | 20 / 54 | 13.6 | 11.6 |
| 100 | yes | 24.6 / 44.5 | 20 / 40 | 18.9 | 6.4 |
| 1,000 | yes | 19.2 / 35.8 | 17 / 35 | 17.8 | 6.3 |
| 10,000 | yes | 26.6 / 47.6 | 17 / 36 | 17.8 | 6.7 |
| 100,000 | yes | 27.9 / 51.2 (56.8 and 51.2 in the other two runs) | 18 / 36 | 18.3 | 7.9 |
| 1M | yes | 20.7 / 39.9 | 17 / 34 | 18.1 | 9.2 |
| 10M | NO after second 100: 1.16M pushed | 19-20 / 36-43 until then | 19 / 34 | 13.7 | 22.7 |

10 partitions: the planner, from the first seconds and before any delete: with dedup on and 150,000 msg/s in each
partition the lanes' time per cycle goes 0.2 → 6 ms as the 60 s window fills (143 cycles a second; lanes at half a
core each, so they wait rather than compute: not looked into). 10M partitions: memory. The leader goes 12 → 22 GB
(the 60 s of dedup, outcomes and rows on top of 10M partitions), free memory falls to 2 GB on the three 32 GB
machines from second 67, one follower falls behind (4.8 → 2 cores, "the append stream took no bytes within 75 ms")
and the p99 goes to 2 s. Alice: say that 10M partitions with dedup does not fit these nodes, no further run.

## 2026-10-13, 09:18-09:38 UTC: shards without code (one raft group against three)

Alice: "What if we implement queue shard?" then "Do it, fast". Three tenants, each with its own queue, in ONE raft
group against the same three tenants in THREE groups (`QUEEN_RAFT_GROUPS=3`, tenant t in group t, leaders
spread), binary `p7`, dedup and retention at 60 s. Launcher `s1.sh` (session scratchpad); rows in
`runs/shards.jsonl`, lines in `runs/shards.out`, runs `sh-single-g1|g3`, `sh-real100k-g1|g3`. Brokers and loaders
are 16 vCPU, 31 GB.

A. One message a request, 3,000 partitions, 60 s steps (last 30 s):

| req/s offered | 1 group: e2e p50 / p99, cores | 3 groups: e2e p50 / p99, cores |
|---|---|---|
| 50,000 | 11.5 / 38.4, 21.0 | 10.4 / 21.5, 29.7 |
| 100,000 | 93.2 / 188.4, 26.6 | 43.0 / 200.7, 35.3 |
| 150,000 | not carried: 124,014 pushed, 1.39M shed | not carried by the rule: 147,785 pushed, 205,086 shed; 38.6 cores |

The ceiling moves from 124,000 to 148,000 req/s (+19%), not by three: with three groups the brokers are at 11.5 /
13.6 / 13.5 of 16 cores, about 260 us of broker CPU for a single-message request across the three nodes, and
the loaders at 40% of theirs. My estimate before the run ("should scale with N") was wrong for req/s.

B. 100 messages a request, 99,999 partitions, 2M msg/s, 200 s (seconds 140-200):

| | 1 group | 3 groups |
|---|---|---|
| e2e p50 / p99 (median window) | 71 / 127 ms, rising (33 → 75) | 38 / 59 ms |
| leader's longest apply per 10 s | 39-103 ms | 12-22 ms |
| durable points per 10 s | 2 | 7 to 35 a group |
| broker cores | 10.3 / 6.9 / 6.7 | 8.7 / 10.8 / 10.5 |

Here three groups halve the latency and bring it to what one group reads with no dedup and no deletes at the same
load (30 / 56). With 2026-10-12's pair (100 messages a request, nothing deleted: 2.5M carried on three groups,
not on one) that is the measured picture: shards cure what is one thread or one store per group (apply with
retention, the checkpoint's pace, the planner cycle), and add about a fifth to the ceilings on three 16-core
machines, for 25-40% more CPU. Not run: transactions (`txnrun.sh` has no tenants or groups). The stand-in has no
pop across shards: a sharded queue pays for that on top.

## 2026-10-13, 10:06-10:36 UTC: Kafka on the same six machines (Alice's plan: 1M, 2M, 4M on 100 partitions, then 2M at every partition count it carries)

September's harness (`../2026-09-30-kafka-pulsar`, `kafka/run.sh`, its `hosts.env` pointed at today's machines),
Kafka with three replicas and `acks=all`, 70 s of load, a fresh cluster per point, launcher `kf.sh`; rows in
`runs/kafka-oct13.out`, raw runs in `../2026-09-30-kafka-pulsar/runs/kafka/*-oct13/`. End-to-end p50 / p99 in ms
at seconds 30-60, the worst process's worst window on both sides (Queen = the clean matrix of this folder,
`runs/grid-final.jsonl`, `mid_e50` / `mid_e99_worst`); cores = the three brokers summed.

| msg/s | partitions | Kafka e2e | Kafka cores | Queen e2e | Queen cores |
|---|---|---|---|---|---|
| 1M | 100 | 8 / 11 | 9.8 | 10 / 19 | 15.1 |
| 2M | 100 | 8 / 43 | 15.8 | 31 / 63 | 19.9 |
| 4M | 100 | 13 / 243 | 21.8 | not carried (2.5M is not carried at 1,000 partitions) | |
| 2M | 10 | 11 / 403 | 10.7 | 44 / 100 | 16.4 |
| 2M | 1,000 | 9 / 25 | 19.5 | 27 / 70 | 20.1 |
| 2M | 10,000 | 64 / 134 | 29.8 | 31 / 63 | 20.2 |
| 2M | 100,000 | not carried: 1.92M pushed, 1.67M consumed, 12k/s shed, 10.4 s / 15.1 s (the rerun, see below) | 33.3 | 30 / 59 | 20.2 |
| 2M | 1,000,000 | not run (the ladder stops at the first point not carried) | | 30 / 66 | 20.4 |

Kafka is ahead to 1,000 partitions (a third of Queen's median at 2M, and it carries 4M where Queen stops before
2.5M); from 10,000 partitions Queen is ahead, flat at 30 / 60 to a million partitions on 20 cores, while Kafka
pays 30 cores at 10,000 and does not carry 100,000 (the topic alone takes 356 to 387 s to create). Kafka's 10
partitions carry the rate with a 403 ms p99 (nine consumers).

The 100,000-partition row is the RERUN of 11:17 UTC (`1x100000-2000k-oct13b`). The first run (10:26 UTC, tag
without the b: 1.74M pushed, 1.37M consumed, 17 s / 26 s) was not a measurement of Kafka: four of its nine kload
processes were killed by the load machines' out-of-memory killer 40 to 86 s into the load (exit code 137, 11 to 16
GB each, three a machine of 31 GB). The rerun gives kload `GOMEMLIMIT=9GiB`, as the 100,000-partition runs of
2026-10-02 had; all nine processes ended with exit code 0, and the end to end p50 rises from 1.5 s in the first
window to 15 s in the last, in every process. The other six points had nine clean processes each.
Consumers: Kafka 99 at 100 partitions, 9 at 10, 297 from 1,000; Queen 297.

The same hour: the committed tree (`e8d460cbf`, 2.3.0-beta.1) was built as an amd64 image and rolled on stage
(helm revision 29, 10:22:15 to 10:26:23 UTC).

## 2026-10-13, 10:56-11:13 UTC: the Kafka facade on the committed build, five tactical points

Alice: "start the queen kafka facade on VMs: only interested in some tactical points to see we have not broke
things". Binary `b1` (the committed tree, 2.3.0-beta.1) with `QUEEN_KAFKA_EMBEDDED=true` on the three brokers, a
fresh cluster per point, the leader on n1; the load is September's `kafka/run.sh TARGET=queen` as on 2026-10-01/02
(`runs/queen-kafka/c3-*`, `c5-*` of that folder): kload, acks=all, idempotent, lz4, classic groups, 9 processes,
`retention.ms=600000`. Launcher `kfac2.sh`, rows in `runs/kfacade-oct13.out`, raw runs in
`../2026-09-30-kafka-pulsar/runs/queen-kafka/b1-*`. End to end p50 / p99 in ms, the worst load process.

| point | pushed / consumed | today (b1) | 10-02 (qk3; qk5 where named) |
|---|---|---|---|
| 300k, 200 partitions | 300k / 300k | 9 / 11 | 10 / 19 |
| 1M, 200 partitions | 1,000k / 1,000k | 12 / 48 | 44 / 108; qk5 35 / 91 |
| 1.5M, 200 partitions | 1,499k / 1,498k | 98 / 175 | 185 / 3,785 |
| 1M, 10,000 partitions | 1,000k / 1,000k | 13 / 26 | 45 / 142 |
| 2M, 200 partitions | 2,007k / 1,992k | 334 / 20,578 | 1,909k in, 105k out, p50 43 s |

No error and nothing shed at any point. Nothing is broken, and every point is better than on 10-02 (other machines
of the same size, so no exact gain is claimed). At 2M the collapse of 10-02 is gone: consumption keeps the rate. Seven
of the nine load processes end at 220 / 350 ms; two hold consumers that fall steadily behind (p99 3 s in the second
window, 18 and 25 s in the last), the "late consumers" seen at 1.5M on 10-02.

Two harness notes. The first launch used kload's default group protocol (`consumer`) and died at group formation
("group-wide ? members=0"): the 10-02 runs were `GROUP_PROTOCOL=classic`. And the 1M point is marked FAILED in the
harness log because the copy of one load machine's files to this Mac failed; all nine processes had ended with exit
code 0, and the row above is from the files fetched afterwards.
