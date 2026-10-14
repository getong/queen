# Perf campaign report

Campaign: `2026-04-21_13-42-20`  
Git: `nagle @ 32ce754` (dirty files: 1)  
Started: 2026-04-21T11:42:20Z  Finished: 2026-04-21T11:45:23.047Z  
Scenarios: 1  
Runs per scenario: 1  Warmup: 10s  Measure: 150s  
Host: 10 logical CPUs, Darwin 24.5.0  

## Headline: throughput & latency (mean ± stddev over runs)

- `push_msgs/s` = push_rps × push_batch (client-side estimate of items produced).
- `pg_ins/s` = `queen.messages` rows inserted/sec measured from `pg_stat_user_tables` (**PUSH ground truth**).
- `pop_upd/s` = `queen.partition_consumers` UPDATEs/sec. Not "pops that got data" directly — the pop procedure does multiple UPDATEs per call (lease, cursor, counters). A proxy for pop activity level.
- `pg_tx/s` = `xact_commit` rate across all PG sessions.
- ⚠️ = host was over-subscribed during this run (more heavy threads than CPUs-2). Interpret with caution.

| # | Scenario | push req/s | push msgs/s | pg_ins/s | pop req/s | pop_upd/s | pg_tx/s | push p99 (ms) | errors p/c | host | Δ pg_ins/s vs #1 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|:---:|---:|
| 1 | `pg2c4g_q2w_prod2w100c` | 858 ± 0 | 8576 | 8649 | 475 | 1054 | 413 | 561 ± 0 | 0 / 0 |  | +0.0% |

## Per-scenario detail

### Scenario 1 — `pg2c4g_q2w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 857.58 | 0.00 | 857.58 | 857.58 |
| push_msgs_per_s | 8575.80 | 0.00 | 8575.80 | 8575.80 |
| push_p50 | 83.00 | 0.00 | 83.00 | 83.00 |
| push_p95 | 419.00 | 0.00 | 419.00 | 419.00 |
| push_p99 | 561.00 | 0.00 | 561.00 | 561.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 474.76 | 0.00 | 474.76 | 474.76 |
| pop_p50 | 36.00 | 0.00 | 36.00 | 36.00 |
| pop_p95 | 188.00 | 0.00 | 188.00 | 188.00 |
| pop_p99 | 281.00 | 0.00 | 281.00 | 281.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=2.0, warnings mean=22.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 8649 | 0 | 8649 | 8649 |
| partition_consumers UPDATEs/s (pop activity) | 1054 | 0 | 1054 | 1054 |
| xact_commit/s | 413 | 0 | 413 | 413 |
| tup_fetched/s | 494462 | 0 | 494462 | 494462 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 2 | 0 | 2 | 2 |
| xact_rollback (delta) | 2 | 0 | 2 | 2 |

Final `queen.messages` rows (mean): 1526050 (max 1526050).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 200.9, max 201.9
- queen-server CPU%: p95 90.3, max 136.8

## How to read this

1. **Primary signal is `pg_ins/s`** (messages actually inserted, measured from `pg_stat_user_tables`). `push msgs/s` is a client-side estimate and can over-count if retries happen or under-count if the server rejects after acknowledge.
2. **`pg_upd/s` ≈ pops + acks** because autoAck updates message rows to completed. If `pop req/s` is high but `pg_upd/s` is low, the consumer is mostly getting empty pops (queue was drained faster than produced).
3. **Treat deltas as noise** unless `|Δ|` exceeds ~2× the reported stddev. The stddev column tells you how reproducible each number is across the 3 runs.
4. **⚠️ host-oversubscribed rows**: the laptop is running more heavy threads than it has CPUs (minus 2). Those numbers reflect OS scheduler contention more than queen/PG behavior — the workload was bottlenecked on the client-side bench, not the server. Ignore for server-side comparisons.
5. **`push_p99` blow-ups** without matching `pg_ins/s` drop mean the server is queuing but not rejecting — look at `queue_full hits` and the `PG ground truth` section.
6. **High `errors`/`non2xx`** invalidate the run. Re-check server log for reconnects/disconnects.
7. **PG CPU at ~`PG_CPUS × 100%`** means PG is the bottleneck. PG CPU far below that + high push_p99 means queen is the bottleneck.
