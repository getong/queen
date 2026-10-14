# Perf campaign report

Campaign: `2026-04-21_11-40-01`  
Git: `master @ fce1833` (dirty files: 5)  
Started: 2026-04-21T09:40:01Z  Finished: 2026-04-21T10:02:16.411Z  
Scenarios: 0, 1, 3  
Runs per scenario: 3  Warmup: 10s  Measure: 120s  
Host: 10 logical CPUs, Darwin 24.5.0  

## Headline: throughput & latency (mean ± stddev over runs)

- `push_msgs/s` = push_rps × push_batch (client-side estimate of items produced).
- `pg_ins/s` = `queen.messages` rows inserted/sec measured from `pg_stat_user_tables` (**PUSH ground truth**).
- `pop_upd/s` = `queen.partition_consumers` UPDATEs/sec. Not "pops that got data" directly — the pop procedure does multiple UPDATEs per call (lease, cursor, counters). A proxy for pop activity level.
- `pg_tx/s` = `xact_commit` rate across all PG sessions.
- ⚠️ = host was over-subscribed during this run (more heavy threads than CPUs-2). Interpret with caution.

| # | Scenario | push req/s | push msgs/s | pg_ins/s | pop req/s | pop_upd/s | pg_tx/s | push p99 (ms) | errors p/c | host | Δ pg_ins/s vs #1 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|:---:|---:|
| 0 | `pg2c4g_q1w_prod2w100c` | 514 ± 208 | 5141 | 5214 | 216 | 490 | 311 | 1196 ± 268 | 0 / 0 |  | +0.0% |
| 1 | `pg2c4g_q2w_prod2w100c` | 317 ± 11 | 3171 | 3165 | 172 | 411 | 363 | 1579 ± 253 | 0 / 0 |  | -39.3% |
| 3 | `pg4c8g_q2w_prod4w200c` | 263 ± 55 | 2630 | 2686 | 140 | 405 | 472 | 6730 ± 1324 | 128 / 0 |  | -48.5% |

## Per-scenario detail

### Scenario 0 — `pg2c4g_q1w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=1, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 514.13 | 207.89 | 376.66 | 753.29 |
| push_msgs_per_s | 5141.27 | 2078.92 | 3766.60 | 7532.90 |
| push_p50 | 142.67 | 52.55 | 82.00 | 174.00 |
| push_p95 | 815.33 | 190.03 | 610.00 | 985.00 |
| push_p99 | 1196.33 | 267.76 | 922.00 | 1457.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 216.20 | 66.76 | 166.60 | 292.11 |
| pop_p50 | 79.33 | 24.01 | 53.00 | 100.00 |
| pop_p95 | 513.00 | 143.82 | 364.00 | 651.00 |
| pop_p99 | 783.33 | 140.83 | 623.00 | 887.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.7, warnings mean=3.7 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 5214 | 2153 | 3842 | 7696 |
| partition_consumers UPDATEs/s (pop activity) | 490 | 137 | 386 | 645 |
| xact_commit/s | 311 | 70 | 263 | 391 |
| tup_fetched/s | 378836 | 143401 | 292345 | 544365 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 1 | 1 | 0 | 2 |
| xact_rollback (delta) | 1 | 1 | 0 | 2 |

Final `queen.messages` rows (mean): 780487 (max 1147450).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 201.1, max 209.0
- queen-server CPU%: p95 81.0, max 110.5

### Scenario 1 — `pg2c4g_q2w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 317.10 | 11.30 | 307.40 | 329.51 |
| push_msgs_per_s | 3171.03 | 113.00 | 3074.00 | 3295.10 |
| push_p50 | 217.00 | 9.85 | 206.00 | 225.00 |
| push_p95 | 1171.67 | 113.78 | 1051.00 | 1277.00 |
| push_p99 | 1579.33 | 253.48 | 1314.00 | 1819.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 171.70 | 8.51 | 166.25 | 181.51 |
| pop_p50 | 85.67 | 2.08 | 84.00 | 88.00 |
| pop_p95 | 657.33 | 54.17 | 595.00 | 693.00 |
| pop_p99 | 1018.00 | 77.35 | 957.00 | 1105.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.3, warnings mean=0.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 3165 | 77 | 3104 | 3251 |
| partition_consumers UPDATEs/s (pop activity) | 411 | 17 | 399 | 430 |
| xact_commit/s | 363 | 3 | 360 | 365 |
| tup_fetched/s | 247635 | 23107 | 222680 | 268290 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 1 | 0 | 1 |
| xact_rollback (delta) | 0 | 1 | 0 | 1 |

Final `queen.messages` rows (mean): 434623 (max 440150).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 203.2, max 208.1
- queen-server CPU%: p95 53.1, max 67.3

### Scenario 3 — `pg4c8g_q2w_prod4w200c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 262.97 | 54.51 | 201.04 | 303.65 |
| push_msgs_per_s | 2629.73 | 545.08 | 2010.40 | 3036.50 |
| push_p50 | 271.00 | 19.97 | 249.00 | 288.00 |
| push_p95 | 5021.33 | 1544.24 | 3816.00 | 6762.00 |
| push_p99 | 6730.33 | 1323.81 | 5454.00 | 8097.00 |
| push_errors | 127.67 | 61.50 | 66.00 | 189.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 139.64 | 30.23 | 105.15 | 161.57 |
| pop_p50 | 175.00 | 11.53 | 163.00 | 186.00 |
| pop_p95 | 2036.67 | 779.74 | 1580.00 | 2937.00 |
| pop_p99 | 2980.33 | 1348.12 | 2197.00 | 4537.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.3, warnings mean=2.3 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 2686 | 548 | 2063 | 3098 |
| partition_consumers UPDATEs/s (pop activity) | 405 | 100 | 291 | 474 |
| xact_commit/s | 472 | 39 | 438 | 514 |
| tup_fetched/s | 248755 | 29743 | 225057 | 282132 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 1 | 0 | 1 |
| xact_rollback (delta) | 0 | 1 | 0 | 1 |

Final `queen.messages` rows (mean): 458323 (max 503150).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 436.9, max 490.8
- queen-server CPU%: p95 94.3, max 153.5

## How to read this

1. **Primary signal is `pg_ins/s`** (messages actually inserted, measured from `pg_stat_user_tables`). `push msgs/s` is a client-side estimate and can over-count if retries happen or under-count if the server rejects after acknowledge.
2. **`pg_upd/s` ≈ pops + acks** because autoAck updates message rows to completed. If `pop req/s` is high but `pg_upd/s` is low, the consumer is mostly getting empty pops (queue was drained faster than produced).
3. **Treat deltas as noise** unless `|Δ|` exceeds ~2× the reported stddev. The stddev column tells you how reproducible each number is across the 3 runs.
4. **⚠️ host-oversubscribed rows**: the laptop is running more heavy threads than it has CPUs (minus 2). Those numbers reflect OS scheduler contention more than queen/PG behavior — the workload was bottlenecked on the client-side bench, not the server. Ignore for server-side comparisons.
5. **`push_p99` blow-ups** without matching `pg_ins/s` drop mean the server is queuing but not rejecting — look at `queue_full hits` and the `PG ground truth` section.
6. **High `errors`/`non2xx`** invalidate the run. Re-check server log for reconnects/disconnects.
7. **PG CPU at ~`PG_CPUS × 100%`** means PG is the bottleneck. PG CPU far below that + high push_p99 means queen is the bottleneck.
