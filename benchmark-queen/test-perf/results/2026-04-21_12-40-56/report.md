# Perf campaign report

Campaign: `2026-04-21_12-40-56`  
Git: `improvements-perf @ 32ce754` (dirty files: 0)  
Started: 2026-04-21T10:40:56Z  Finished: 2026-04-21T11:03:16.571Z  
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
| 0 | `pg2c4g_q1w_prod2w100c` | 654 ± 152 | 6545 | 6640 | 232 | 524 | 146 | 813 ± 201 | 0 / 0 |  | +0.0% |
| 1 | `pg2c4g_q2w_prod2w100c` | 619 ± 145 | 6186 | 6185 | 314 | 719 | 235 | 884 ± 274 | 0 / 0 |  | -6.8% |
| 3 | `pg4c8g_q2w_prod4w200c` | 471 ± 98 | 4714 | 4654 | 243 | 669 | 257 | 3681 ± 1356 | 2 / 0 |  | -29.9% |

## Per-scenario detail

### Scenario 0 — `pg2c4g_q1w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=1, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 654.48 | 151.67 | 544.97 | 827.60 |
| push_msgs_per_s | 6544.83 | 1516.71 | 5449.70 | 8276.00 |
| push_p50 | 108.33 | 25.72 | 79.00 | 127.00 |
| push_p95 | 556.33 | 131.31 | 437.00 | 697.00 |
| push_p99 | 813.33 | 201.22 | 629.00 | 1028.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 231.55 | 54.84 | 183.50 | 291.29 |
| pop_p50 | 79.00 | 16.37 | 61.00 | 93.00 |
| pop_p95 | 430.67 | 132.83 | 310.00 | 573.00 |
| pop_p99 | 697.00 | 219.15 | 495.00 | 930.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=1.7, warnings mean=14.7 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 6640 | 1516 | 5571 | 8375 |
| partition_consumers UPDATEs/s (pop activity) | 524 | 115 | 422 | 649 |
| xact_commit/s | 146 | 13 | 134 | 160 |
| tup_fetched/s | 440298 | 84954 | 388148 | 538327 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 2 | 1 | 1 | 2 |
| xact_rollback (delta) | 2 | 1 | 1 | 2 |

Final `queen.messages` rows (mean): 1003463 (max 1243540).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 200.6, max 208.9
- queen-server CPU%: p95 91.9, max 115.2

### Scenario 1 — `pg2c4g_q2w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 618.63 | 144.88 | 482.60 | 770.98 |
| push_msgs_per_s | 6186.30 | 1448.81 | 4826.00 | 7709.80 |
| push_p50 | 120.33 | 31.09 | 99.00 | 156.00 |
| push_p95 | 580.00 | 156.77 | 399.00 | 673.00 |
| push_p99 | 884.00 | 273.93 | 624.00 | 1170.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 314.32 | 61.93 | 258.83 | 381.12 |
| pop_p50 | 54.33 | 10.41 | 46.00 | 66.00 |
| pop_p95 | 308.00 | 77.00 | 220.00 | 363.00 |
| pop_p99 | 482.67 | 116.50 | 354.00 | 581.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=1.7, warnings mean=16.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 6185 | 1482 | 4841 | 7774 |
| partition_consumers UPDATEs/s (pop activity) | 719 | 128 | 614 | 861 |
| xact_commit/s | 235 | 24 | 221 | 263 |
| tup_fetched/s | 387819 | 86573 | 336868 | 487779 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 2 | 2 | 0 | 4 |
| xact_rollback (delta) | 2 | 2 | 0 | 4 |

Final `queen.messages` rows (mean): 949593 (max 1221680).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 203.2, max 218.0
- queen-server CPU%: p95 135.5, max 193.6

### Scenario 3 — `pg4c8g_q2w_prod4w200c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 471.36 | 98.16 | 361.31 | 549.90 |
| push_msgs_per_s | 4713.57 | 981.63 | 3613.10 | 5499.00 |
| push_p50 | 204.67 | 24.54 | 190.00 | 233.00 |
| push_p95 | 2447.33 | 800.04 | 1833.00 | 3352.00 |
| push_p99 | 3680.67 | 1355.62 | 2302.00 | 5012.00 |
| push_errors | 2.33 | 4.04 | 0.00 | 7.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 242.98 | 48.84 | 189.78 | 285.79 |
| pop_p50 | 125.00 | 28.62 | 107.00 | 158.00 |
| pop_p95 | 1014.67 | 240.21 | 812.00 | 1280.00 |
| pop_p99 | 1496.33 | 339.24 | 1139.00 | 1814.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.3, warnings mean=10.7 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 4654 | 1032 | 3497 | 5481 |
| partition_consumers UPDATEs/s (pop activity) | 669 | 134 | 521 | 780 |
| xact_commit/s | 257 | 32 | 221 | 282 |
| tup_fetched/s | 296513 | 41406 | 269177 | 344151 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 1 | 0 | 1 |
| xact_rollback (delta) | 0 | 1 | 0 | 1 |

Final `queen.messages` rows (mean): 791983 (max 836360).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 433.8, max 523.7
- queen-server CPU%: p95 136.3, max 162.3

## How to read this

1. **Primary signal is `pg_ins/s`** (messages actually inserted, measured from `pg_stat_user_tables`). `push msgs/s` is a client-side estimate and can over-count if retries happen or under-count if the server rejects after acknowledge.
2. **`pg_upd/s` ≈ pops + acks** because autoAck updates message rows to completed. If `pop req/s` is high but `pg_upd/s` is low, the consumer is mostly getting empty pops (queue was drained faster than produced).
3. **Treat deltas as noise** unless `|Δ|` exceeds ~2× the reported stddev. The stddev column tells you how reproducible each number is across the 3 runs.
4. **⚠️ host-oversubscribed rows**: the laptop is running more heavy threads than it has CPUs (minus 2). Those numbers reflect OS scheduler contention more than queen/PG behavior — the workload was bottlenecked on the client-side bench, not the server. Ignore for server-side comparisons.
5. **`push_p99` blow-ups** without matching `pg_ins/s` drop mean the server is queuing but not rejecting — look at `queue_full hits` and the `PG ground truth` section.
6. **High `errors`/`non2xx`** invalidate the run. Re-check server log for reconnects/disconnects.
7. **PG CPU at ~`PG_CPUS × 100%`** means PG is the bottleneck. PG CPU far below that + high push_p99 means queen is the bottleneck.
