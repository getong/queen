# Perf campaign report

Campaign: `2026-04-21_16-33-03`  
Git: `nagle @ 574622e` (dirty files: 7)  
Started: 2026-04-21T14:33:03Z  Finished: 2026-04-21T14:55:16.785Z  
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
| 0 | `pg2c4g_q1w_prod2w100c` | 901 ± 146 | 9011 | 9026 | 274 | 567 | 86 | 473 ± 36 | 0 / 0 |  | +0.0% |
| 1 | `pg2c4g_q2w_prod2w100c` | 703 ± 73 | 7034 | 7052 | 304 | 663 | 138 | 618 ± 56 | 0 / 0 |  | -21.9% |
| 3 | `pg4c8g_q2w_prod4w200c` | 720 ± 33 | 7203 | 7221 | 229 | 531 | 120 | 1156 ± 208 | 0 / 0 |  | -20.0% |

## Per-scenario detail

### Scenario 0 — `pg2c4g_q1w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=1, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 901.13 | 146.34 | 808.08 | 1069.81 |
| push_msgs_per_s | 9011.30 | 1463.41 | 8080.80 | 10698.10 |
| push_p50 | 88.67 | 19.66 | 66.00 | 101.00 |
| push_p95 | 319.00 | 11.14 | 309.00 | 331.00 |
| push_p99 | 472.67 | 36.23 | 439.00 | 511.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 273.57 | 36.55 | 252.43 | 315.78 |
| pop_p50 | 73.33 | 14.15 | 57.00 | 82.00 |
| pop_p95 | 293.00 | 22.65 | 269.00 | 314.00 |
| pop_p99 | 423.67 | 45.65 | 392.00 | 476.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.3, warnings mean=0.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 9026 | 1503 | 8036 | 10756 |
| partition_consumers UPDATEs/s (pop activity) | 567 | 73 | 524 | 651 |
| xact_commit/s | 86 | 5 | 82 | 92 |
| tup_fetched/s | 404162 | 72861 | 359152 | 488223 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 1 | 0 | 1 |
| xact_rollback (delta) | 0 | 1 | 0 | 1 |

Final `queen.messages` rows (mean): 1391143 (max 1676040).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 197.5, max 202.2
- queen-server CPU%: p95 96.8, max 125.1

### Scenario 1 — `pg2c4g_q2w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 703.38 | 72.57 | 619.78 | 750.20 |
| push_msgs_per_s | 7033.77 | 725.71 | 6197.80 | 7502.00 |
| push_p50 | 110.33 | 11.85 | 103.00 | 124.00 |
| push_p95 | 447.00 | 58.62 | 401.00 | 513.00 |
| push_p99 | 618.00 | 56.32 | 576.00 | 682.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 303.90 | 19.34 | 288.33 | 325.55 |
| pop_p50 | 59.33 | 3.51 | 56.00 | 63.00 |
| pop_p95 | 279.00 | 16.37 | 261.00 | 293.00 |
| pop_p99 | 412.00 | 43.03 | 368.00 | 454.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=1.3, warnings mean=9.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 7052 | 686 | 6262 | 7497 |
| partition_consumers UPDATEs/s (pop activity) | 663 | 40 | 629 | 706 |
| xact_commit/s | 138 | 7 | 130 | 143 |
| tup_fetched/s | 404003 | 51405 | 352439 | 455247 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 1 | 2 | 0 | 3 |
| xact_rollback (delta) | 1 | 2 | 0 | 3 |

Final `queen.messages` rows (mean): 1154750 (max 1230530).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 200.1, max 202.1
- queen-server CPU%: p95 158.0, max 177.9

### Scenario 3 — `pg4c8g_q2w_prod4w200c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 720.31 | 32.74 | 689.55 | 754.72 |
| push_msgs_per_s | 7203.07 | 327.39 | 6895.50 | 7547.20 |
| push_p50 | 217.00 | 7.21 | 211.00 | 225.00 |
| push_p95 | 881.67 | 113.85 | 811.00 | 1013.00 |
| push_p99 | 1156.00 | 208.47 | 1001.00 | 1393.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 228.54 | 17.49 | 208.51 | 240.77 |
| pop_p50 | 137.33 | 1.53 | 136.00 | 139.00 |
| pop_p95 | 1065.00 | 156.92 | 957.00 | 1245.00 |
| pop_p99 | 1549.67 | 231.25 | 1362.00 | 1808.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=0.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 7221 | 350 | 6852 | 7548 |
| partition_consumers UPDATEs/s (pop activity) | 531 | 37 | 490 | 559 |
| xact_commit/s | 120 | 5 | 115 | 125 |
| tup_fetched/s | 529649 | 35394 | 488820 | 551638 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 0 | 0 | 0 |
| xact_rollback (delta) | 0 | 0 | 0 | 0 |

Final `queen.messages` rows (mean): 1189217 (max 1213190).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 393.4, max 409.6
- queen-server CPU%: p95 173.5, max 195.0

## How to read this

1. **Primary signal is `pg_ins/s`** (messages actually inserted, measured from `pg_stat_user_tables`). `push msgs/s` is a client-side estimate and can over-count if retries happen or under-count if the server rejects after acknowledge.
2. **`pg_upd/s` ≈ pops + acks** because autoAck updates message rows to completed. If `pop req/s` is high but `pg_upd/s` is low, the consumer is mostly getting empty pops (queue was drained faster than produced).
3. **Treat deltas as noise** unless `|Δ|` exceeds ~2× the reported stddev. The stddev column tells you how reproducible each number is across the 3 runs.
4. **⚠️ host-oversubscribed rows**: the laptop is running more heavy threads than it has CPUs (minus 2). Those numbers reflect OS scheduler contention more than queen/PG behavior — the workload was bottlenecked on the client-side bench, not the server. Ignore for server-side comparisons.
5. **`push_p99` blow-ups** without matching `pg_ins/s` drop mean the server is queuing but not rejecting — look at `queue_full hits` and the `PG ground truth` section.
6. **High `errors`/`non2xx`** invalidate the run. Re-check server log for reconnects/disconnects.
7. **PG CPU at ~`PG_CPUS × 100%`** means PG is the bottleneck. PG CPU far below that + high push_p99 means queen is the bottleneck.
