# Perf campaign report

Campaign: `2026-04-21_14-02-51`  
Git: `nagle @ 32ce754` (dirty files: 1)  
Started: 2026-04-21T12:02:51Z  Finished: 2026-04-21T12:11:43.853Z  
Scenarios: 1  
Runs per scenario: 3  Warmup: 10s  Measure: 150s  
Host: 10 logical CPUs, Darwin 24.5.0  

## Headline: throughput & latency (mean ± stddev over runs)

- `push_msgs/s` = push_rps × push_batch (client-side estimate of items produced).
- `pg_ins/s` = `queen.messages` rows inserted/sec measured from `pg_stat_user_tables` (**PUSH ground truth**).
- `pop_upd/s` = `queen.partition_consumers` UPDATEs/sec. Not "pops that got data" directly — the pop procedure does multiple UPDATEs per call (lease, cursor, counters). A proxy for pop activity level.
- `pg_tx/s` = `xact_commit` rate across all PG sessions.
- ⚠️ = host was over-subscribed during this run (more heavy threads than CPUs-2). Interpret with caution.

| # | Scenario | push req/s | push msgs/s | pg_ins/s | pop req/s | pop_upd/s | pg_tx/s | push p99 (ms) | errors p/c | host | Δ pg_ins/s vs #1 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|:---:|---:|
| 1 | `pg2c4g_q2w_prod2w100c` | 855 ± 157 | 8550 | 8573 | 415 | 923 | 389 | 552 ± 178 | 0 / 0 |  | +0.0% |

## Per-scenario detail

### Scenario 1 — `pg2c4g_q2w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 854.97 | 156.56 | 677.56 | 973.74 |
| push_msgs_per_s | 8549.73 | 1565.57 | 6775.60 | 9737.40 |
| push_p50 | 88.67 | 13.61 | 78.00 | 104.00 |
| push_p95 | 404.33 | 109.92 | 334.00 | 531.00 |
| push_p99 | 551.67 | 177.64 | 434.00 | 756.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 415.06 | 55.14 | 355.91 | 465.04 |
| pop_p50 | 43.33 | 5.51 | 38.00 | 49.00 |
| pop_p95 | 212.33 | 38.99 | 181.00 | 256.00 |
| pop_p99 | 310.33 | 69.95 | 259.00 | 390.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=1.0, warnings mean=4.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 8573 | 1565 | 6808 | 9788 |
| partition_consumers UPDATEs/s (pop activity) | 923 | 97 | 822 | 1015 |
| xact_commit/s | 389 | 21 | 366 | 405 |
| tup_fetched/s | 424407 | 85182 | 334258 | 503553 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 1 | 0 | 1 |
| xact_rollback (delta) | 1 | 0 | 1 | 1 |

Final `queen.messages` rows (mean): 1620880 (max 1805760).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 201.2, max 203.4
- queen-server CPU%: p95 133.1, max 162.6

## How to read this

1. **Primary signal is `pg_ins/s`** (messages actually inserted, measured from `pg_stat_user_tables`). `push msgs/s` is a client-side estimate and can over-count if retries happen or under-count if the server rejects after acknowledge.
2. **`pg_upd/s` ≈ pops + acks** because autoAck updates message rows to completed. If `pop req/s` is high but `pg_upd/s` is low, the consumer is mostly getting empty pops (queue was drained faster than produced).
3. **Treat deltas as noise** unless `|Δ|` exceeds ~2× the reported stddev. The stddev column tells you how reproducible each number is across the 3 runs.
4. **⚠️ host-oversubscribed rows**: the laptop is running more heavy threads than it has CPUs (minus 2). Those numbers reflect OS scheduler contention more than queen/PG behavior — the workload was bottlenecked on the client-side bench, not the server. Ignore for server-side comparisons.
5. **`push_p99` blow-ups** without matching `pg_ins/s` drop mean the server is queuing but not rejecting — look at `queue_full hits` and the `PG ground truth` section.
6. **High `errors`/`non2xx`** invalidate the run. Re-check server log for reconnects/disconnects.
7. **PG CPU at ~`PG_CPUS × 100%`** means PG is the bottleneck. PG CPU far below that + high push_p99 means queen is the bottleneck.
