# Three machines with the new wire forms, and what stops one raft group, 2026-10-12

The branch `perfimpr` adds a binary push body, a counts answer for acks, a pop answer made of the
stored records, and a checkpoint written out in steps (commits `7c7c0f1a8` to `56f96d33d`). These
runs measure them on three brokers against 2.2.0, look for what stops one raft group, and measure
one fix for it (`1d0981c57`).

On the same machines, on a queue of 200 partitions, 2.2.0 pushes at most 1.57M messages a second
and the branch 2.45M. At 1M a second the push takes 8.1 / 39.9 ms (median / p99) on 2.2.0 and
5.2 / 8.7 ms on the branch, and the three brokers use 16.4 cores against 13.4.

One run per point, steps of 45 to 75 seconds. Jepsen was not run on any of this code.

## The machines

Six DigitalOcean droplets in one private network, each 16 vCPU and 31 GB, Ubuntu 24.04 (kernel
6.8), ext4 root disk. They are the same size and not the same machine:

| Droplet | CPU | One thread, 20M-step loop | 32 KB write + fdatasync, p50 / p99 | Syncs a second |
|---|---|--:|--:|--:|
| queen | Xeon Gold 6548N | 1.44 s | 0.08 / 0.29 ms | 10,100 |
| queen-04 | Xeon Gold 6548N | 1.39 s | 0.72-0.94 / 6.3-14.4 ms | 585-1,080 |
| queen-05 | Xeon Gold 6548N | 1.44 s | 0.76-0.95 / 2.6-3.2 ms | 1,000-1,170 |
| queen-02 | Xeon Platinum 8358 | 2.05 s | 0.10 / 0.40 ms | 8,090 |
| queen-03 | Xeon Platinum 8358 | 2.04 s | 0.09 / 0.37 ms | 9,240 |
| queen-06 | Xeon Platinum 8358 | 2.14 s | 0.20 / 1.17 ms | 2,760 |

The first three runs (`smoke`, `final-g1`, `final-1x200`) had the three Gold machines as brokers:
the faster cores, and two disks that sync ten times slower than the third. Every later run has
queen (the leader), queen-02 and queen-03 as brokers and the other three as loaders.
`scripts/fsync.py` is the disk test; run it before giving roles.

The disks changed more than the code did. Same build, 500,000 partitions, 1M a second: push
14.1 / 57.9 ms with the slow followers, 5.8 / 11.5 ms with the fast ones. On 200 partitions at
300k a second the push p99 was 242 ms and all of it belonged to the clients of one follower
(15 ms through the other two nodes, 324 ms through it): a node answers a write it forwarded only
when it holds the entry itself. The numbers of 2026-10-11 came from droplets whose disks nobody
measured, so nothing here is compared with them.

## The builds

`binary.md5` lists the four. All are `cargo build --release --locked` with rustc 1.99.0, made on
the brokers: `220` (the tree of release 2.2.0), `new` (`4f8119304`), `b32` and `b32c` (the dedup
filter fix, `1d0981c57`).

Every broker runs with `QUEEN_RAFT_TXN_WINDOW_MIN_S=60`. The new builds also run with
`QUEEN_RAFT_DURABLE_EVERY_MS=250` and `QUEEN_RAFT_CHECKPOINT_FLUSH_STEP_MB=8`; 2.2.0 runs with its
defaults, and how much of its p99 the first of those two settings alone would move was not
measured.

## The load

`qload` of [`../2026-09-30-kafka-pulsar/mqload`](../2026-09-30-kafka-pulsar/mqload), nine
processes on three loaders, open loop: pushes of 100 JSON events of 256 B, consumers that pop
1,000 messages over 10 partitions and ack without waiting. Dedup window 60 s, lease 30 s,
completed messages kept 60 s. The new builds are driven with
`-push-wire bin -push-zstd -ack-counts -pop-frames`, 2.2.0 with the JSON forms.

Two layouts: 500,000 partitions over three tenants (the layout of 2026-10-11), and one queue of
200 partitions with 22 consumers a process (series A of the Kafka comparison).

A rate is carried when nothing is shed, nothing errs and 99% of what is pushed is consumed.
Latencies are the nine processes' histograms pooled (`scripts/mreport.py`).

## 200 partitions

Push and end to end, median / p99 in ms. The last line is what the cluster pushes when it is
offered more than it can take.

| msg/s | 2.2.0, JSON (`base220-1x200`) | `4f8119304` (`disks-1x200`) | `1d0981c57` (`b32-1x200`) |
|---|---|---|---|
| 300k | 3.2 / 5.7, 8.1 / 14.2 | 3.1 / 5.2, 7.1 / 11.5 | |
| 600k | 4.7 / 22.0, 12.4 / 43.0 | 4.0 / 6.3, 9.1 / 14.5 | |
| 1.0M | 8.1 / 39.9, 27.1 / 85.0 | 5.9 / 10.1, 13.2 / 27.6 | 5.2 / 8.7, 11.5 / 18.4 |
| 1.5M | 20.7 / 78.8, 76.8 / 213 | 9.1 / 20.0, 25.6 / 58.4 | |
| 2.0M | not carried | not carried | 12.7 / 110, 37.4 / 197 |
| 2.5M | | | 66.6 / 223, 172 / 533 |
| most pushed | 1.57M | 1.85M | 2.45M |

At 1M the three brokers use 7.8 / 4.4 / 4.2 cores on 2.2.0 and 6.0 / 3.6 / 3.4 on `4f8119304`;
the leader sends 4.7 Gbit/s against 2.4; the loaders use 14.4 cores against 8.3.

## 500,000 partitions

| msg/s | `4f8119304`, one group (`disks-g1`) | `1d0981c57`, one group (`b32c-g1`) | `4f8119304`, three groups (`g3`) |
|---|---|---|---|
| 1.0M | 5.8 / 11.5, 13.2 / 44.5 | 6.0 / 11.8, 13.7 / 46.1 | 5.2 / 10.1, 11.1 / 47.1 |
| 1.9M | 20.7 / 73.7, 46.6 / 274 | 17.2 / 47.6, 37.4 / 122 | 10.0 / 20.7, 21.2 / 55.3 |
| 2.2M | not carried | not carried | |
| 2.5M | | | 21.5 / 75.8, 46.6 / 240 |
| 3.0M | | | not carried |
| most pushed | 2.14M | 2.17M | 2.96M, 2.76M consumed |

Three groups are three tenants, one raft group each, a group led by each node
(`QUEEN_RAFT_GROUPS=3`). They carry a third more than one group and give nothing at 1M, for 38
to 50% more broker CPU at the same rate (22.6 cores against 14.9 at 1M). At 3M the three machines
are nearly full: 11 to 13.5 of 16 cores each and 877 MB/s written to each disk. 2.2.0 was not run
on this layout.

## What stops one group

The planner. One planning cycle runs at a time, and at the limit there is always one running.

On 200 partitions with `4f8119304` (probe `probes/c1x200`, taken on the leader at the limit by
`scripts/ceilprobe.sh`): 179 cycles a second of 5.1 ms each, 91% busy, while the disk is at 20%
utilisation, the log's sync thread is in `fdatasync` 42% of the time and the leader uses 9.7 of
its 16 cores. The call graphs of the lane threads (`probes/lanes-cg`) put 62% of their time in
`Planner::dedup_committed` -> `scan_txns_range_for_hash`: the lookup that follows a "maybe" of the
dedup filter. 0.34 to 0.40% of the messages paid one, and one cost about 330 us, because the
filter of a partition is cut in slices of a quarter of the window and a "maybe" scans every row of
the slice: about 1,400 rows of 100 hashes for a partition at 9,500 messages a second. That is why
200 partitions met it and 500,000 did not.

`1d0981c57` sizes the filter of a busy partition at 32 bits a hash instead of 16. The lookups fall
to 0.026% of the messages, a cycle to 4.2 ms, and the limit rises from 1.85M to 2.45M. The filter
grows from 0.56 to 1.23 GB in the run, so its cap went from 2 to 4 GiB.

At the new limit it is the planner again (`probes/c1x200-b32c`, `probes/cycletrace-b32c-1x200.txt`):
238 cycles a second of 4.2 ms, every one started by a commit and none by an arrival, about 3,460
commands always waiting. Of the 4.2 ms, 2.7 are the wait for the lanes (their work is 1.0 ms on
average and 1.7 for the slowest, and the last lane starts about 1.1 ms late) and about 1.2 are
serial work around them. That is about 26,000 pushes of 100 messages a second. The disk is at 24
to 34%, the six raft threads at 25% each or less, the four apply threads at 24% or less. The
500,000-partition layout stops at the same place (`probes/cg1-b32c`).

Four settings were tried at that limit, and none raises it:

| Setting | Run | Result |
|---|---|---|
| `QUEEN_LANES=16` (default 8), before the fix | `lanes16-1x200` | 1.99M against 1.89M: the lanes' work halves, the cycle does not |
| `QUEEN_RAFT_PIPELINE=16` (default 8) | `b32c-1x200-pipe16` | the same 2.46M; a commit takes 38 ms instead of 21 |
| `QUEEN_RAFT_BATCH_MAX_BYTES` 8 MB (default 4) | `b32c-1x200-batch8` | the same commands a second in cycles twice as long; consumers fall behind |
| `QUEEN_LANES_SPIN_US=5000` (default 0) | `b32c-1x200-spin` | the late start falls to 0.4 ms and the cluster pushes 1.86M: eight spinning threads take 5.6 cores |

Not done: the late start of the lanes without spinning, less serial work per command around the
lanes, and the zstd pass the raft threads make over entries the client already compressed (17% of
their time).

## Transactions

The transactional pipeline of [`../2026-09-30-kafka-pulsar/txn`](../2026-09-30-kafka-pulsar/txn)
on this harness (`scripts/txnrun.sh`): a feeder fills an input queue, 198 workers each commit one
transaction per batch (every input acked with its lease and the outputs pushed), readers consume
the output. 200 partitions, 40 s steps, the fast-disk brokers. `edeb8b1e8` adds a binary
transaction body whose pushes are frames the client built and compressed; the runs compare
2.2.0, that build with JSON transactions, and that build with the binary body. No step had an
error or an abort. Commit and end to end are median / p99 in ms.

| Transactions of 10 messages | 2.2.0 (`txn-220-10`) | JSON body (`txn-json-10`) | Binary body (`txn-bin-10`) |
|---|---|---|---|
| 6,000 txn/s: commit | 2.8 / 5.0 | 2.7 / 4.5 | 2.6 / 4.2 |
| 12,000 txn/s: commit | 5.3 / 12.4 | 4.9 / 11.1 | 3.6 / 8.6 |
| 12,000 txn/s: end to end | 78 / 195 | 72 / 176 | 62 / 143 |
| txn/s when 18,000 are offered | 11,320 | 11,770 | 12,890 |

| Transactions of 100 messages | 2.2.0 (`txn-220-100`) | JSON body (`txn-json-100`) | Binary body (`txn-bin-100`) |
|---|---|---|---|
| 3,000 txn/s: commit | 6.1 / 19.2 | 5.6 / 9.7 | 4.1 / 7.0 |
| 3,000 txn/s: end to end | 25 / 60 | 21 / 30 | 19 / 26 |
| 3,000 txn/s: broker cores | 5.8 / 3.8 / 3.6 | 5.1 / 3.5 / 3.2 | 4.8 / 3.3 / 3.1 |
| txn/s when 6,000 are offered | 2,020 | 2,290 | 2,450 |

The binary body takes about a third off the commit time at the highest rate all three carry, and
moves the ceiling by 14% (10 messages) and 21% (100 messages) against 2.2.0.

The ceilings belong to the design and are the same in the three. With 10 messages all stop
between 11,000 and 13,000 transactions a second with the leader at about 6 of 16 cores: a worker
manages about 65 a second, a loop of about 15 ms made of its pop, which is a lease committed
through raft, and its commit, and a partition is held by one worker at a time. With 100 messages
all stop near 2,000 to 2,450 on the planner's serial control phase, where every transaction that
carries an ack is planned: it takes 5.5 ms of each 6.5 ms cycle and its thread is at 0.86 of a
core. Past that limit the cluster does less than at it (3,000 a second are carried, 2,450 are
reached when 6,000 are offered) and plain pushes wait behind the control phase for 0.6 to 0.7 s.

`scripts/txncheck.sh` is the check of correctness: the same pipeline on one loader through every
broker, then qload's verifier over the id ledgers. Both bodies: every message once, none missing
(`txncheck-bin`, `txncheck-json`).

## What was read wrongly on the way

- The first comparison was with the numbers of 2026-10-11 ("1.2M carried, now 1.6M"). Most of that
  difference was disks and the layout.
- "The leader's raft thread is pinned" and "the apply thread is at 90%": both are several threads
  whose times the sampler adds up (six and four). None is near a full core.
- At the second limit the commit pipeline looked full (6 to 8 entries in flight of 8). Raising the
  bound to 16 moved nothing.

## The files

`scripts/`: `mrun.sh` (one fresh cluster, then a step per rate; `QBIN`, `QEXTRA`, `EXTRA_ENV`,
`TEN`, `PARTS`, `CONS`), `ld.sh`, `qc.sh`, `deploy.sh`, `bins.sh`, `mreport.py`, `step.py`,
`threads.py`, `ceilprobe.sh` with `synctrace.bt` and `pthreads.py`, `fsync.py`, `pattern.py`,
`txnrun.sh` with `txnstep.py`, `txncheck.sh`.
They come from [`../2026-10-10-three-node-1m/scripts`](../2026-10-10-three-node-1m/scripts).

`runs/<run>/<rate>/` holds the nine load processes' reports and the brokers' samples;
`runs/<run>/run.log` the run's own lines. `probes/` holds what `ceilprobe.sh` wrote on the leader,
without the raw `perf` files.
