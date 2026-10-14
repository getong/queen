# stage-workload: a small paced workload for the stage Queen cluster

`wl` (main.go, Go standard library only) pushes at a fixed rate into 5 queues with 10,000 partitions in total
(`wl-0` .. `wl-4`, 2,000 partitions each, `p0` .. `p1999`), consumes them (queue mode, pop + ack) and prints
messages per second and latency every 10 seconds, then a result table.

`run.sh` runs it as one pod (`queen-workload`, namespace `queen`) inside the stage cluster. The pod builds main.go
from a ConfigMap, so no image is pushed. The kubectl context is pinned to stage.

```
./run.sh start                           # 1,000 msg/s for 10 minutes
./run.sh start -rate 2000 -duration 5m   # any wl flag
./run.sh logs                            # follow it
./run.sh prove                           # the 2.2.0 memory rule, about 58 minutes (below)
./run.sh top                             # broker and loader CPU / memory
./run.sh mem                             # rows in RAM and memory of each broker
./run.sh stop                            # end now, the result table is still printed
./run.sh clean                           # delete the five queues, the pod and the ConfigMap
```

Against another broker: `GOWORK=off go run . -url http://127.0.0.1:6632 -rate 500 -duration 30s`.

## Proving the 2.2.0 memory rule: `./run.sh prove`

The rule (2.2.0-beta.2 on): every push leaves one row of about 200 bytes in the RAM of every node, and the row
stays for the larger of the queue's `dedupWindowSeconds` and the broker's floor `QUEEN_RAFT_TXN_WINDOW_MIN_S`
(900 s). A few seconds later it is deleted. RAM = push rate x window. A message older than that is read from the
queue log. `completedRetentionSeconds` keeps messages on disk and no row; in 2.2.0-beta.1 it was a third term of the
window (`-completed-holds-rows` models that build).

What the run does, and what the rule predicts for it (1,000 pushes a second, 200 per queue):

| queue | settings | row window | why |
|---|---|---|---|
| wl-0, wl-1 | dedup 0, completed retention 60 | 900 s | the dedup window is under the floor |
| wl-2 | dedup 0, completed retention 1,800 | 900 s | the retention holds no row |
| wl-3 | dedup 1,200 | 1,200 s | the dedup window |
| wl-4 | dedup 1,500, no consumer until the end | 1,500 s | the dedup window |

| time since START | rows in RAM on every broker |
|---|---|
| 0 to 900 s | +1,000 a second, 900,000 at 900 s |
| 900 to 1,200 s | +400 a second (three queues are flat), 1,020,000 at 1,200 s |
| 1,200 to 1,500 s | +200 a second, 1,080,000 at 1,500 s |
| 1,500 to 1,800 s | flat at 1,080,000 while the pushes go on |
| pushes end at 1,800 s | -1,000 a second for 900 s, -400 for 300 s, -200 for 300 s |
| 3,300 s (+5) | 0 |
| 3,360 s | wl-4 gets its first consumers: 360,000 messages, none with a row left |

Every report line carries the proof: `rows a/b/c` is `queen_raft_store_ram_rows{ks="txns",kind="live"}` of each
broker minus what was there before START, `model` is the count the rule gives for this run's own acked pushes, and
`cold claims` is `queen_consume_cold_claims_total` (claims served from the queue log). `./run.sh mem` prints each
broker's anonymous memory, for the bytes per row. The run stops pushing by itself if a broker's resident memory
passes 1,800 MB (`-stop-rss-mb`). Stage's own queues move the baseline by up to about 2,000 rows in an hour.

The flags behind it: `-brokers` (each broker's own URL), `-dedup-window` and `-completed-retention` (one value, or
one per queue), `-floor`, `-linger` (keep reporting after the last push), `-late-queues` (queues read only after the
linger), `-warm=false` (so that the rows are the run's pushes and nothing else).

### Four runs on stage, 2026-10-10 (3 nodes, floor 900 s)

| run | START | build | queue windows | folder under `runs/` |
|---|---|---|---|---|
| 1 | 07:05:17Z | 2.2.0-beta.1 | 900, 900, 900, 1,200 (completed retention), 1,500 (dedup) | `2026-10-10-prove-stage` |
| 2 | 08:08:02Z | beta.1, rolled to beta.2 at 08:26-08:29Z (minute 18 to 21) | the same settings | `2026-10-10-prove-stage-2` |
| 3 | 09:06:00Z | 2.2.0-beta.2 | 900, 900, 900 (completed retention 1,800), 1,200 and 1,500 (dedup) | `2026-10-10-prove-stage-3` |
| 4 | 10:07:15Z | 2.2.0-beta.2, the processes of run 3 | as run 3 | `2026-10-10-prove-stage-4` |

Rows measured (the three brokers agree to within two rows) against each build's rule:

| time since START | run 1 measured | beta.1 rule | run 2 measured | run 3 measured | beta.2 rule for runs 2 / 3 |
|---|---|---|---|---|---|
| 900 s | 900,010 | 900,000 | 900,014 | 899,967 | 900,000 |
| 1,200 s | 1,021,847 | 1,020,000 | 1,020,439 | 1,020,460 | 1,020,000 |
| 1,210 s, a beta.2 node leads | | | 962,197 | | 962,000 |
| 1,500 s | 1,082,452 | 1,080,000 | 1,020,215 | 1,080,587 | 1,020,000 / 1,080,000 |
| 1,800 s, the last push | 1,083,053 | 1,080,000 | 1,020,741 | 1,080,760 | 1,020,000 / 1,080,000 |
| 2,100 s | 783,107 | 780,000 | 720,749 | 780,784 | 720,000 / 780,000 |
| 2,700 s | 183,142 | 180,000 | 120,717 | 180,808 | 120,000 / 180,000 |
| 3,000 s | 61,283 | 60,000 | 58,527 | 60,310 | 60,000 |
| 3,310 s | 52 | 0 | -2,033 | -23 | 0 |

- Run 2 saw the rule change while it ran: when a beta.2 node took the lead (08:28:02 to 08:28:12Z), the 58,000 rows
  of wl-3 older than 900 s left in one step, and the plateau became 1,020,000. The roll itself cost 2 failed ack
  calls (their messages came again after the 30 s lease), 0 failed pushes, a push maximum of 75 ms.
- Run 3: wl-2 keeps its consumed messages 1,800 s and its rows still left at 900 s (1,200,000 at 1,500 s otherwise).
- While rows leave, the measured count is about 3 seconds of them above the rule.
- The late queue, read one minute after the last row had left: 360,000 of 360,000 in 15.0 / 14.1 / 15.2 s
  (24,000 to 25,600 msg/s), with 5,413 / 5,431 / 5,453 claims served from the queue log.
- Each run: 1,800,000 messages at 1,000 a second, 0 failed, 0 shed, push p50 4.4 ms, p99 8.1 to 8.6 ms.

Run 4 repeated run 3 on the same processes: 900,020 at 900 s, 1,020,274 at 1,200 s, 1,080,323 at 1,500 s,
1,080,396 at the last push, 180,462 at 2,700 s, 3 at the end; the late queue 360,000 of 360,000 in 14.8 s with 5,415
claims from the log; 0 failed, 0 shed.

Anonymous memory per broker (0 / 1 / 2), MB:

| | run 1 | run 2 (restarted at minute 18 to 21) | run 3 | run 4 |
|---|---|---|---|---|
| before START | 170 / 173 / 227 | 300 / 309 / 376 | 314 / 320 / 391 | 343 / 348 / 422 |
| plateau (1,500 s) | 490 / 502 / 557 | 564 / 521 / 587 | 555 / 553 / 621 | 547 / 558 / 630 |
| the last push (1,800 s) | 478 / 490 / 552 | 563 / 528 / 589 | 541 / 540 / 630 | 545 / 538 / 628 |
| 3,000 s, 60,000 rows left | 481 / 478 / 523 | 464 / 468 / 533 | 502 / 504 / 570 | 505 / 497 / 578 |
| no row left, the late queue read | 322 / 322 / 374 | 312 / 314 / 387 | 349 / 353 / 430 | 374 / 364 / 449 |
| the queues deleted | 300 / 313 / 382 | 313 / 313 / 391 | 346 / 349 / 430 | 370 / 367 / 459 |

- Bytes per row where rows are only added (run 1, 166 s to 900 s): 199 to 215.
- Flat at the plateau while the pushes go on: bounded by rate x window.
- The level a broker goes back to after a run rose each time: 130 to 155 MB after the first run, then 33 to 39 MB
  (run 3 against run 2) and 18 to 29 MB (run 4 against run 3), with no row of the runs left in any keyspace of the
  store.
- The level it reaches under load did not: the plateau and the last push of run 4 are within 9 MB of run 3's,
  although run 4 started about 30 MB higher. Memory that leaked would be on top of the plateau too. So this reads as
  freed memory the process keeps, less each run, and not as a leak; no heap profile was taken to confirm it.

The same cycle in three minutes, on a broker of your own with a 20-second floor:

```
docker run -d -e QUEEN_RAFT_TXN_WINDOW_MIN_S=20 -p 127.0.0.1:17732:6632 ghcr.io/queen-mq/queen:2.2.0-beta.2
GOWORK=off go run . -url http://127.0.0.1:17732 -brokers http://127.0.0.1:17732 -rate 500 -duration 70s -linger 60s \
  -report 5s -warm=false -floor 20 -dedup-window 0,0,0,35,50 -completed-retention 10,10,60,10,10 -late-queues 1
```

## What the numbers mean

- **push latency**: from the instant a push was SCHEDULED to the broker's answer. The pacer is open loop: it never
  waits for an answer, so a slow broker shows up as latency (and, above `-max-inflight` 512 pushes in flight, as
  `shed`), not as a lower offered rate.
- **end to end**: from the scheduled instant to the pop answer that carries the message. Same process, same clock.
- **backlog / left**: pushed minus consumed-and-acked.
- One message per push by default (`-batch`), ~256 B (`-payload`), partitions taken round robin, so at 1,000 msg/s
  every partition gets a message every 10 seconds.
- Before the measured run every partition gets one warm-up message (not counted), so the run meets partitions
  that exist.

The queues are configured with retention: consumed messages go after 300 s, unconsumed after 1 hour
(`-completed-retention`, `-retention`), and with `dedupWindowSeconds` 0 (`-dedup-window`): a queue nobody configured
has 3,600, and its rows would stay in RAM for an hour. `./run.sh clean` removes the queues at once.

Main flags: `-queues 5 -partitions 10000 -rate 1000 -batch 1 -payload 256 -consumers 4 -pop-batch 100 -pop-width 10
-duration 10m -report 10s`. `go run . -h` lists all of them.

## The same matrix on 2.3.0 (2026-10-14, stage, 3 nodes)

Same loader (JSON pushes and pops), same shape, 60 s a point, 07:11-07:20Z; the loader on a node without a broker
(on 10-09 it shared a follower's node, which was full at 10,000 msg/s). Logs and `table.md` in
`runs/2026-10-14-matrix-230/`. Latency in ms.

| offered msg/s | pushed msg/s | shed | push p50 / p99: 2.1.0 -> 2.3.0 | end to end p50 / p99: 2.1.0 -> 2.3.0 | leader cores: 2.1.0 -> 2.3.0 | follower cores, 2.3.0 |
|---|---|---|---|---|---|---|
| 200 | 200 | 0 | 5.1 / 8.0 -> 4.8 / 7.1 | 9.7 / 16.2 -> 8.3 / 11.8 | 1.55 -> 1.67 | 1.03 / 0.99 |
| 500 | 500 | 0 | 4.3 / 6.9 -> 4.5 / 7.5 | 10.3 / 15.0 -> 8.6 / 13.6 | 2.19 -> 2.41 | 1.54 / 1.44 |
| 1,000 | 1,000 | 0 | 4.7 / 9.7 -> 4.7 / 10.3 | 11.3 / 18.6 -> 9.7 / 20.2 | 3.04 -> 3.12 | 2.06 / 1.89 |
| 2,000 | 2,000 | 0 | 6.4 / 21.8 -> 5.9 / 12.5 | 15.3 / 66.1 -> 13.0 / 25.1 | 3.68 -> 3.72 | 2.61 / 2.64 |
| 10,000, 20 consumers x 10 partitions | 8,179 (2.1.0: 9,754) | 18% (2.5%) | 23.1 / 92.6 -> 45.4 / 102 | 771 / 1,994 -> 1,168 / 2,528 | 4.64 -> 3.17 | 1.96 / 1.88 |
| 10,000, 20 x 64 partitions | 8,303 (2.1.0: 9,627) | 17% (3.7%) | 26.1 / 98.3 -> 47.2 / 94.5 | 67 / 726 -> 111 / 193 | 4.72 -> 3.44 | 2.15 / 2.04 |

0 failed pushes, 0 messages left, 0 errors at every point. Shed = pushes the loader did not send because 512 were
in flight.

- Up to 2,000 msg/s 2.3.0 is as fast as 2.1.0 at the push and faster end to end (p99 at 2,000: 66 -> 25 ms), on
  the same CPU.
- At 10,000 msg/s it carries less: 8,200 msg/s, push p50 45-47 ms, while the brokers use less CPU than 2.1.0 did.
  The leader's own timings say where it waits: its raft log fsync went from 853 a second at 0.96 ms (2,000 msg/s)
  to 74 a second at 11.2 ms (10,000), 82% busy both times, and its checkpoint write from 4 a second at 48 ms (19%
  busy) to 2 a second at 548-608 ms (98-99% busy). 56 heartbeat WARNs on the leader (no answer in 75 ms), all
  inside the two 10,000 msg/s minutes; no election.
- The disk, measured the same morning (`runs/2026-10-14-disk-10k/`: each broker's `/proc/diskstats` once a
  second, one 10,000 msg/s minute with 64-partition pops, START 07:26:15Z):

  | seconds after START | write MB/s | write operations a second | ms a write | disk busy | requests queued | pushed msg/s, push p50 |
  |---|---|---|---|---|---|---|
  | before | 0.3 | 52-56 | 1.0 | 1% | 0.1 | |
  | 10-35 (medians, leader / followers) | 54 / 55 | 7,770 / 7,860-8,030, rising to 11,900 at 32 s | 1.1-1.4 | 46-56% | 9-11 | 9,570-9,940, 23 ms |
  | 40-58 (medians) | 54-55 | 7,680-7,720, flat on all three | 7.5-8.0 | 90-93% | 56-61 | 7,860-8,230, 49-52 ms |

  The write operations a second rise for 32 seconds at a constant push rate, then all three volumes stop at about
  7,700 a second, the time per write goes from 1.2 to 8 ms and the queue from 10 to 60: the pushes slow at that
  second. Each message costs about 5.5 KB of writes for a 256-byte payload, so most of the writes are the store's
  checkpoint, not the logs (a reading from the sizes, not separated by a counter). 2.2.0 wrote that checkpoint once
  a second, 2.3.0 every 250 ms (`QUEEN_RAFT_DURABLE_EVERY_MS`).
- The same minute with `QUEEN_RAFT_DURABLE_EVERY_MS=1000` on the three brokers (deploy repo `4ced69e`, helm
  revision 32, rolled 07:31-07:35Z; START 07:37:51Z; `runs/2026-10-14-disk-10k-checkpoint-1s/`):

  | checkpoint every | pushed msg/s | shed | push p50 / p99 | end to end p50 / p99 | seconds 40-58: writes a second, ms a write, disk busy, queued |
  |---|---|---|---|---|---|
  | 250 ms (2.3.0 default) | 9,098 | 9% | 30.6 / 94.5 | 77 / 193 | 7,680-7,720, 7.5-8.0 ms, 90-93%, 56-61 |
  | 1,000 ms | 9,923 | 0.8% | 17.9 / 63.6 | 45 / 146 | 8,370-8,770, 1.8-2.3 ms, 49-53%, 15-19 |

  No wall with the one-second checkpoint: the disks wrote more (72 MB/s against 55, more operations too) at half
  the busy time, and every 10-second window held 9,770-10,010 msg/s at a push p50 of 15.6-23.6 ms. The 7,700 writes a
  second were not the volume's ceiling but where its small writes queued up. One minute each, one run each.

## First numbers (2026-10-09, stage, Queen 2.1.0, 3 nodes)

One 60-second run per rate, 5 queues x 2,000 partitions, one ~256 B message per push, 4 consumers per queue. The
loader pod ran on the node of one follower. Stage's own traffic ran alongside. Latency in ms. Broker CPU is each
process's per-thread CPU time (/proc) over a 45-second window inside the run; idle is about 0.1 core per broker.

| offered msg/s | failed / shed / left | push p50 | push p99 | push max | end to end p50 | end to end p99 | end to end max | leader cores | follower cores | raft entries/s |
|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 0 / 0 / 0 | 5.1 | 8.0 | 13 | 9.7 | 16.2 | 22 | 1.55 | 0.97 / 0.99 | 373 |
| 500 | 0 / 0 / 0 | 4.3 | 6.9 | 13 | 10.3 | 15.0 | 49 | 2.19 | 1.49 / 1.35 | 699 |
| 1,000 | 0 / 0 / 0 | 4.7 | 9.7 | 22 | 11.3 | 18.6 | 46 | 3.04 | 2.04 / 2.07 | 1,121 |
| 2,000 | 0 / 0 / 0 | 6.4 | 21.8 | 217 | 15.3 | 66.1 | 389 | 3.68 | 2.60 / 2.66 | 1,530 |
| 10,000 | 0 / 14,747 / 0 | 23.1 | 92.6 | 287 | 771 | 1,994 | 2,618 | 4.64 | 3.58 / 2.98 | 800 |

The leader wrote 13 commands per raft entry at 10,000 msg/s (2 at 2,000) and its log fsync was 74% busy at 1.5 ms.

### 10,000 msg/s: three consumer shapes

Each pop of the first run took about 27 ms and returned 13 messages, so its 20 consumers carried about 9,700 msg/s,
the offered rate itself. Two more one-minute runs, with `-completed-retention 30`:

| consumers x partitions per pop | flags | pushed msg/s | shed | push p50 | push p99 | end to end p50 | end to end p99 | backlog during the run | messages per pop | leader cores | loader cores |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20 x 10 | (defaults) | 9,754 | 2.5% | 23.1 | 92.6 | 771 | 1,994 | 5,500-8,000 | 13.2 | 4.64 | not recorded |
| 80 x 10 | `-consumers 16` | 9,225 | 7.7% | 35.1 | 115 | 89 | 1,058 | 700-2,300 | 6.3 | 4.86 | 2.95 |
| 20 x 64 | `-pop-width 64` | 9,627 | 3.7% | 26.1 | 98.3 | 67 | 726 | 5,200 falling to 900 | 19.8 | 4.72 | 2.57 |

0 failed pushes and 0 messages left in all three. "Shed" is pushes the loader did not send because 512 were already
in flight. Wider pops (64 partitions, the broker's maximum) cut the end-to-end median from 771 ms to 67 ms at the
same number of consumers; more consumers do it too, but their extra calls slow the pushes.

**These 10,000 msg/s numbers come from a full node.** The loader pod sat on the node of follower `queen-mq-v2-1`:
loader 2.6-3.0 cores + that broker 3.0-3.6 + about 1.5 of other pods = the node's 8 vCPU. Latency is measured from
the scheduled instant, so the loader's own delay is inside every number. `run.sh` now keeps the loader off broker
nodes; the 10,000 msg/s points were not repeated with it.

### Memory on stage (limit 2 GiB per broker)

Anonymous memory per broker, MB (pod 0 / 1 / 2):

| when | anon |
|---|---|
| after the 200-2,000 msg/s runs (276,000 messages) | 341 / 348 / 450 |
| after the first 10,000 msg/s minute (595,000 messages) | 713 / 704 / 843 |
| 5 minutes later, retention 300 s just over | 618 / 632 / 742 |
| after two more 10,000 msg/s minutes | 1,064 / 1,094 / 1,264 |
| 2 minutes after the queues were deleted | 531 / 601 / 673 |

During the last two runs the pods' total memory (anonymous + page cache) stood at the 2 GiB limit (2,081-2,145 MB)
and the kernel was giving page cache back. No pod restarted. **Do not run this rate for more than a minute at a
time on stage**, and check `./run.sh top` between runs.

At 2,000 msg/s the first 20 seconds had an end-to-end
p99 of 100-189 ms; the last 40 seconds 27-32 ms. The cause was not looked for.

Leader CPU by thread group at 200 / 2,000 msg/s: raft threads 0.75 / 1.34 cores, HTTP workers 0.33 / 1.00, apply
0.07 / 0.26.
