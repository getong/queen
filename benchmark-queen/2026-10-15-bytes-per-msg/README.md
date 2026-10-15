# Bytes per message: where Queen's 249 B/msg go (2026-10-15)

The 10-14 GCP runs (3M msg/s, 1,000 partitions, 3 nodes, XFS) wrote 746 MB/s per Queen node = 249 B/msg, and
440 MB/s per Kafka broker = 147 B/msg (`../2026-10-12-three-node-new-forms/runs/cs-p1000-r3000000`,
`runs/kafka-linger0.out`). This rig takes one node's bytes apart.

## Rig

One 2.3.0 broker (`ghcr.io/queen-mq/queen:2.3.0-arm64`, openraft, one voter) in a privileged OrbStack container,
its data directory on an ext4 loop device; qload with the GCP flags (`-batch 100 -payload 256 -pop-batch 1000
-pop-width 10 -push-wire bin -push-zstd -ack-counts -pop-frames`, dedup 0, completed retention 1800 s,
`QUEEN_RAFT_TXN_WINDOW_MIN_S=60`) at a tenth of the scale with the same load per partition: 300k msg/s on 100
partitions, 30 consumers. Once a second the sampler (`tool/`, `bpm sample`) records the loop device's sectors, the
process and per-thread `write_bytes`/`wchar`, file sizes by kind and the broker's push counter. After the run the
broker stops cleanly and `bpm parse` walks every `.qlog` record (message records, entry records, commands,
outcomes, effects), `bpm frames` recompresses a sample of the payload frames without their ids, `bpm lmdb` walks
`data.mdb` page by page.

    cd scripts/tool && GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../bpm-linux .
    (cd ../../2026-09-30-kafka-pulsar/mqload && GOWORK=off go build -o ../../2026-10-15-bytes-per-msg/scripts/qload ./cmd/qload)
    scripts/run.sh a 100 300000 30 150            # BATCH=79 PARSE=lmdb ... for the other runs
    python3 scripts/analyze.py scripts/out/a/sample.jsonl 15-45 70-100

## Result (run `a`, 42.2M messages, window 15-45 s: before the 60 s windows expire, like GCP's 30-60 s)

| Part | B/msg |
|---|---:|
| Payload as stored: the client's zstd frames | 104.5 |
| of which the message id twice: 16-byte id 15.2, its 36-char text as `transactionId` 12.5 | 27.7 |
| Queue log: push outcome in the entry record, 25 B per message (`tag pid offset created_at`) | 25.0 |
| Queue log: the 16-byte hash in the message record | 16.0 |
| Queue log: the same hash again in the entry record's `Append` | 16.0 |
| Queue log: cursor effects 2.0, record and entry headers 1.7 | 3.6 |
| LMDB checkpoint (`queen-rsm-ckpt` writes) | 90.2 (98 once rows expire) |
| `.qidx`, re-dirtied tail pages, raft files | 3.6 |
| **Process total (`write_bytes`)** | **259.2** |
| ext4 journal on top (block device 278.7); XFS on GCP adds about 1 (inferred) | 19.5 |

LMDB per keyspace at the end of the run (`runs/a/lmdb.txt`): `txns` 148,980 rows on 148,963 leaf pages (1.00 rows
per leaf, 40% full, 1,628-byte values); `request_ids` 140,811 overflow pages for the push rows (2.5 KB outcome each).
Every 100-message push therefore costs two whole 4 KiB pages = 82 B/msg before any rewrite. Cause of the one row per
leaf: on an append at the end of a full leaf, LMDB's split moves the last row together with the new one to the new
page (`mdb.c` 9969-9997 in lmdb-master-sys 0.2.6), so rows above a third of a page end up alone. Batch 79 (the
outcome row fits inline, 2.0 KB) and batch 80 (it spills) cost the same: LMDB 91.3 and 89.2 B/msg, about 7.1 KB
per push either way. Size is the problem, not overflow.

Kafka for the same messages: lz4 payload 138.3 B/msg plus about 9.

## Changes that keep every guarantee (savings at the GCP shape)

1. Compact push outcome (`pid, base_offset, count, created_at` for the created run, duplicates listed): -65 B/msg.
2. `txns` hashes out of the LMDB checkpoint, reloaded from the queue log at boot: about -42 B/msg (its page, plus
   part of the 8-16 B/msg of rewrites).
3. Entry record without the hash list (the message record beside it holds the same bytes): -16 B/msg.
4. `transactionId` stored once when it is the id's text: -12.5 B/msg.

From GCP's 249: 184 after 1, 142 after 1-2 (0.97x Kafka), 126 after 1-3, about 114 after all four (0.78x Kafka).

## Fixes 1-3 built and measured (run `fx`, same rig and load, image built from this worktree)

`scripts/run4.sh fx 100 300000 30 150` with `IMAGE=queen-bpm:fixes123 WANT_CV=7` (the cluster version reached 7
before the load); 43.5M messages, 0 shed, 0 errors. `python3 scripts/compare.py runs/a/sample.jsonl
runs/fx/sample.jsonl 15-45 70-100 110-150`:

| B/msg | before (`a`) | after (`fx`) |
|---|---:|---:|
| Process total (`write_bytes`), 15-45 s | 259.2 | 145.6 |
| Process total, 110-150 s (rows expiring) | 267.7 | 141.4 |
| Queue log (file growth) | 165.5 | 126.1 |
| LMDB checkpoint | 90.2 | 12.0 |

What the parse and the page walk show (`runs/fx/parse.txt`, `runs/fx/lmdb.txt`): push outcome 25.04 -> 0.37 B/msg;
126,084 of 137,091 entry records written without hash lists (`append/hashes` 16.00 -> 0); `request_ids` 590 -> 27 MB
(30 rows a leaf, 56-byte values), `txns` 616 -> 11 MB (55 rows a leaf, 28-byte values). Against GCP's 249 B/msg the
same saving gives about 135, 0.92x Kafka's 147 (projection; measured on the VMs below: 146 to 148).

## Fixes 1-3 on the GCP NVMe VMs: 3,000,000 msg/s on 1,000 partitions (runs `runs/gcp/`)

The six VMs of 2026-10-14 made again (`scripts/gcp/create.sh`): three brokers c4-standard-24-lssd, the four local
NVMe disks in a RAID0 with XFS (noatime), three loaders c4-highcpu-16; a 32 KB write and `fdatasync` on each
broker's RAID 16,700 to 17,700 a second, p50 0.05 to 0.06 ms. The 2026-10-12 harness (`mrun.sh`) with the settings
of 2026-10-14's `cs-p1000-r3000000`: one queue of 1,000 partitions, nine load processes, 256-byte payloads, binary
push with zstd, ack counts, framed pops, dedup off, completed retention 1,800 s, one 70 s step at 3,000,000 msg/s
(`scripts/gcp/ab.sh`). Two runs of each broker, alternating stock and fix. Stock is the 2.3.0 image's binary (the
prod digest); fix is this worktree built on broker 1. Every 20 s each broker's `/health` was read: cluster version 6
in the stock runs, 7 in the fix runs from the first read (`runs/gcp/probe-cv.log`). All four carried the rate with
nothing shed and no error. Disk = the RAID's bytes written, seconds 30-60; latency = qload's last 30 s pooled, and
the docs' statistic (the slowest process's three windows of seconds 30-60, their median); `scripts/gcp/abtable.py`.

| | stock 2.3.0 (`bpm1`, `bpm2`) | fixes 1-3 (`bpm1`, `bpm2`) |
|---|---|---|
| disk B/msg, n1 / n2 / n3 | 248.7 / 247.9 / 248.5; 248.2 / 248.1 / 247.9 | 148.2 / 145.8 / 146.5; 148.1 / 145.8 / 146.4 |
| disk MB/s a broker | 744 to 746 | 437 to 444 |
| push p50 / p99 ms, pooled | 8.3 / 22.3; 9.2 / 24.3 | 3.2 / 7.0; 3.2 / 6.7 |
| e2e p50 / p99 ms, pooled | 19.2 / 43.5; 21.0 / 46.6 | 6.7 / 15.0; 6.6 / 14.2 |
| e2e p50 / p99 ms, docs statistic | 19.1 / 61.2; 21.1 / 45.8 | 6.7 / 17.3; 6.6 / 14.9 |
| cores n1 / n2 / n3 | 11.3 / 5.3 / 5.7; 11.2 / 5.3 / 5.6 | 12.6 / 5.9 / 6.1; 12.6 / 5.8 / 6.0 |
| memory GB n1 / n2 / n3 | 9.9 / 9.4 / 9.4; 10.0 / 9.5 / 9.5 | 5.2 / 4.8 / 4.8; 5.2 / 4.8 / 4.8 |

For reference, 2026-10-14 on the same shapes: stock `cs-p1000-r3000000` 248.9 / 248.4 / 248.3 B/msg, e2e 19.1 / 44.3
(docs statistic); Kafka 4.3.1 on the docs page 443 MB/s a broker (147 B/msg), e2e 6.0 / 8.1, 21.0 cores.

The leader's own timings (`end-n1.txt`, the broker's apply-path line, means over each 10 s at full load):

| leader, each 10 s at full load | stock | fixes 1-3 |
|---|---|---|
| queue-log `fdatasync`, mean (max) | 2.9 to 3.4 ms (23 to 33) | 0.48 to 0.52 ms (4 to 9) |
| commit, mean | 4.0 to 4.7 ms | 1.08 to 1.14 ms |
| entries committed | 12,400 to 14,700 | 43,700 to 45,700 |
| replication RPCs | 10,800 to 13,700 | 64,000 to 68,800 |

What it says:

- Bytes: 41% less on every broker, 248 to 146-148 B/msg, the same 440 MB/s a broker as Kafka at this load.
- Latency: the log's `fdatasync` went from about 3 ms to 0.5 ms, six times faster for 41% fewer bytes, and push and
  e2e latency fell about three times with it. The RAID's own time per write (`/sys/block/md0/stat`, write ticks over
  write requests, seconds 30-60, `scripts/gcp/wlat.py`) went from 0.54-0.59 ms to 0.04-0.06 ms on every broker, at
  24-34% busy against 34-37%: the writes no longer queue at the device. The likely reason, NOT tested in isolation:
  the checkpoint no longer writes two scattered 4 KiB pages per push (90 B/msg on the local rig, not split out on
  the VMs; synced every 250 ms) on the disks the log syncs on, and the group commit then settles on smaller groups
  (3.2 times the entries a second). One run of the fix with `QUEEN_RAFT_CHECKPOINT_TXNS=whole` (fix 2 off) would
  split fix 2's share from the rest.
- CPU: 2.2 cores more in all (22.3 to 24.5), 1.3 of them on the leader, mostly its raft runtime (2.15 to 2.66
  cores), which follows the entries and RPCs a second above. The followers' checkpoint thread went from 0.21 to 0.09
  cores; the leader's is not recorded in the fix runs (the sampler lists the 14 busiest threads and it fell below).
- Memory: 4.7 GB less a broker, the `request_ids` rows no longer holding a 2.5 KB outcome each.

## The ceiling on 1,000 partitions with fixes 1-3 (runs `runs/gcp/fxc-p1000-r*`)

`grid3.sh 1000` as 2026-10-14's stock ladder (`gcp24-p1000-r*`, the docs' ceiling table): a fresh cluster a rate,
70 s, judged on the last 30 s by `gstep.py` (nothing shed, no error, 99.5% of the offered rate pushed, 99% of that
consumed); from 3,500,000 in steps of 500,000, stopping at the first rate not carried; the brokers' disks trimmed
first (`scripts/gcp/ceil.sh`). e2e by the docs' statistic, p50 / p99 (worst window's p99); cores of the three brokers;
disk a broker, seconds 30-60. `scripts/gcp/ceiltable.py`.

| msg/s | stock 2.3.0 (2026-10-14) | fixes 1-3 |
|---|---|---|
| 3,000,000 | 20.4 / 50.4 (78), 22.2 cores, 757 MB/s | 6.6 / 14.9 (16) and 6.7 / 17.3 (18), 24.5 cores, 440 MB/s (the A/B above) |
| 3,500,000 | 45.3 / 96.8 (98), 26.2 cores, 863 MB/s | 10.0 / 27.8 (31), 25.8 cores, 493 MB/s |
| 4,000,000 | not carried: 3,950,000 pushed, 2.0M shed, e2e 1.2 s | 16.1 / 52.0 (52), 28.4 cores, 550 MB/s |
| 4,500,000 | not run | 26.0 / 62.2 (68), 31.4 cores, 609 MB/s |
| 5,000,000 | not run | 39.2 / 86.5 (97), 35.5 cores, 669 MB/s |
| 5,500,000 | not run | not carried: 5,060,000 pushed, 24.5M shed, push p50 0.9 s |

The ceiling on 1,000 partitions goes from 3,500,000 to 5,000,000 msg/s, with the p99 under 100 ms at 5,000,000.
At 5,500,000 the push latency sat at the loaders' in-flight cap (0.9 s) while the brokers carried 5,060,000; the
leader used 19.0 of 24 cores and each broker wrote 670 MB/s, as at 5,000,000 and half what its RAID takes (about
1,340), so the disks are not what stopped it; what did was not looked into (no profile). Kafka 4.3.1 on the docs page carries
8,000,000 on 1,000 partitions (5.8 / 12.5) and 10,000,000 by the rule with a p99 of 375 ms.

## The partitions matrix at 3,000,000 msg/s with fixes 1-3 (runs `runs/gcp/fm3-p*-r3000000`)

2026-10-14's `m3` matrix again (the docs' 3,000,000 msg/s table), the fixed broker: `grid3.sh` with 70 s steps, a
fresh cluster a point, 10, 100, 1,000, 10,000, 100,000 and 1,000,000 partitions, then 10,000,000 with
`QUEEN_CONSUME_IDLE_UNLOAD_S=30` and the 240 s predrain, as `m3` (`scripts/gcp/m3fix.sh`); disks trimmed before the
first point and before 10,000,000. All seven carried with nothing shed and no error; cluster version 7 on every node
at the end of every point. e2e p50 / p99 by the docs' statistic (worst window's p99), cores of the three brokers;
the stock column is recomputed from the `m3` runs with the same script (`scripts/gcp/m3table.py`), Kafka's is the
docs page's.

| Partitions | Queen 2.3.0 | Queen, fixes 1-3 | Kafka 4.3.1 | disk a broker, 2.3.0 -> fixes | leader memory GB |
|---|---|---|---|---|---|
| 10 | 36.1 / 76.3 (78), 18.6 cores | 10.2 / 24.2 (27), 21.0 cores | 6.4 / 11 (45), 14.9 cores | 237 -> 138 B/msg | 10.0 -> 5.2 |
| 100 | 20.9 / 54.0 (65), 23.0 | 7.5 / 17.3 (17), 27.7 | 6.0 / 7.5 (8), 18.5 | 241 -> 139 | 10.0 -> 5.1 |
| 1,000 | 20.6 / 47.4 (56), 22.4 | 12.0 / 64.8 (72), 25.9 | 6.0 / 8.1 (8), 21.0 | 252 -> 141 | 10.1 -> 5.4 |
| 10,000 | 24.2 / 61.2 (66), 23.3 | 7.1 / 20.4 (26), 24.8 | 8.4 / 21 (78), 52.8 | 306 -> 186 | 10.3 -> 5.6 |
| 100,000 | 23.4 / 52.5 (71), 23.4 (one pop error) | 7.1 / 14.8 (17), 25.0 | not carried | 290 -> 153 | 11.2 -> 6.0 |
| 1,000,000 | 22.9 / 42.2 (47), 23.0 | 7.5 / 20.4 (21), 24.9 | not run | 249 -> 145 | 12.6 -> 7.7 |
| 10,000,000 | 23.2 / 46.3 (54), 23.4 | 8.9 / 24.7 (29), 25.4 | not run | 244 -> 143 | 21.8 -> 17.2 |

- The median is 7 to 12 ms against 21 to 36, about three times lower except at 1,000 partitions (1.7 times), near
  Kafka's 6 to 8.
- The p99 is 15 to 25 ms at six of the seven counts, against 42 to 76. Kafka's is still lower from 10 to 1,000
  partitions (7.5 to 11 ms); at 10,000 the two are level (20.4 and 21) with Queen on 24.8 cores against 52.8.
- The 1,000-partition point is the exception, 12.0 / 64.8: the A/B above measured the same shape twice at 6.6 / 14.9
  and 6.7 / 17.3. Not the disks: the RAID took 0.05-0.06 ms a write at 17-19% busy in that run, as fast as in the
  A/B. All three nodes' log syncs were slower than in the A/B from the first seconds (mean 0.54-0.79 ms against
  0.35-0.48, `scripts/gcp/flushcmp.sh`) and the leader committed half the entries a second (21,000 every 10 s
  against 45,000): the group commit settled on fewer, larger groups. Why, not found. Two more runs of the point
  (`fm3r1`, `fm3r2`, `scripts/gcp/rep1k.sh`, fresh cluster each) gave 6.8 / 20.1 (21) and 7.2 / 18.0 (21), with log
  syncs of 0.50-0.52 ms on the leader: five runs of the shape give a p99 of 15, 17, 18, 20 and 65 ms, so 65 is the
  odd one, and the charts draw the median of the five.
- CPU: 1.5 to 4.7 cores more than 2.3.0 (the most at 100 partitions), as in the A/B.
- The 10,000,000 point's predrain logged 596 pop errors, as 2.3.0's did (594): the prefill drain, not the step.

## The 1 KB matrix at 1,000,000 msg/s with fixes 1-3 (runs `runs/gcp/f1k-p*-r1000000`)

2026-10-14's `p1k` matrix again (the docs' 1 KB table): 1,000,000 msg/s of 1,024-byte JSON payloads (1,027 bytes on
average), everything else as the 3M matrix, the fixed broker (`scripts/gcp/m1kfix.sh`); disks trimmed before the
first point and before 10,000,000. All seven carried with nothing shed and no error; cluster version 7 on every node.
Same statistic; the stock column recomputed from the `p1k` runs (`scripts/gcp/m1ktable.py`), Kafka's from the docs.

| Partitions | Queen 2.3.0 | Queen, fixes 1-3 | Kafka 4.3.1 | disk a broker, 2.3.0 -> fixes | leader memory GB |
|---|---|---|---|---|---|
| 10 | 15.6 / 46.8 (68), 13.8 cores | 9.7 / 20.4 (24), 15.2 cores | 7.9 / 9.4 (9), 9.2 cores | 666 -> 538 B/msg | 4.1 -> 2.3 |
| 100 | 9.8 / 30.3 (34), 17.4 | 6.7 / 19.1 (19), 18.2 | 7.4 / 9.4 (10), 9.5 | 672 -> 540 | 4.0 -> 2.4 |
| 1,000 | 9.2 / 26.0 (27), 16.1 | 6.8 / 16.2 (17), 17.1 | 7.4 / 9.4 (9), 11.9 | 708 -> 557 | 5.0 -> 3.3 |
| 10,000 | 9.9 / 27.5 (28), 16.2 | 6.9 / 16.5 (17), 17.4 | 8.8 / 19 (20), 31.4 | 721 -> 569 | 5.4 -> 3.7 |
| 100,000 | 10.7 / 26.5 (27), 16.5 | 6.8 / 20.4 (21), 17.5 | not carried | 707 -> 549 | 6.1 -> 4.2 |
| 1,000,000 | 10.7 / 26.2 (29), 16.0 | 7.4 / 17.3 (17), 17.4 | not run | 670 -> 545 | 8.0 -> 6.5 |
| 10,000,000 | 9.9 / 27.5 (65), 16.5 | 7.0 / 22.1 (25), 17.7 | not run | 675 -> 545 | 16.0 -> 14.1 |

- The median is 6.7 to 9.7 ms against 9.2 to 15.6, the p99 16 to 22 ms against 26 to 47; at 10,000 partitions the
  p99 is under Kafka's (16.5 against 19) on 17.4 cores against 31.4. Kafka keeps the lower p99 from 10 to 1,000
  partitions (9.4 ms).
- Bytes: 19 to 22% fewer (538 to 569 B/msg against 666 to 721): with 1 KB messages the payload is most of what is
  written, so the same per-message saving is a smaller share.
- CPU: 0.8 to 1.4 cores more than 2.3.0. The 10,000,000 point's predrain logged 594 pop errors, as 2.3.0's (594).

## Charts (`charts/`)

- `queen-3m-23-vs-24-{light,dark}.{svg,png}` (`charts/m3data.py`, then `charts/m3_23_vs_24_svg.py`): the 3M matrix,
  Queen 2.3 (2026-10-14's `m3`) against 2.4 (`fm3`): e2e p50 and p99, then cores, leader memory, disk writes and the
  disk's time a write. 1,000 partitions: the median of each version's five runs of that shape.
- `queen24-kafka-3m-{light,dark}.{svg,png}` (`charts/queen24_kafka_3m_svg.py <the 10-12 folder's charts/>`): the
  docs' Queen vs Kafka chart (`queen_kafka_3m_svg.py` of 2026-10-14, its Kafka rows and statistic untouched) with the
  Queen 2.4 rows; 1,000 partitions the median of the five runs.
- `bench-queen24-e2e-{light,dark}.{svg,png}` (`charts/readme24_svg.py`): the README's chart
  (`assets/bench-queen-e2e-*.svg`, 2.3.0) with Queen 2.4's runs, same axes: the worst window's p99 at 3M x 256 B
  (27, 17, 21, 26, 17, 21, 29 ms from 10 to 10M partitions; 1,000 = the median of five) and 1M x 1 KB (24, 19, 17, 17,
  21, 17, 25), against 2.3.0's 78, 65, 56, 66, 71, 47, 54 and 68, 34, 27, 28, 27, 29, 65. Drop-in for the README's
  `<picture>` when 2.4 ships; the README is not changed.

Not a lever: the durable point at 1000 ms (run `d1000`) wrote 88.0 B/msg to LMDB against 90.2 at 250 ms, because
each push's two pages are written once whatever the cadence (that run also shed 8% on this Mac).

Seen on the way, not part of the 1.7x: at 50k msg/s (run `smoke`) the queue-log writers wrote twice what the files
hold (335 vs 169 B/msg): the zero-filled preallocation ahead of the active file, on by design while a sync carries
under 32 KiB (`qlog/mod.rs`, `PREALLOC_MAX_BYTES_PER_SYNC`). At 300k msg/s it is off (166 vs 165).
