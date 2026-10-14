# Perf campaign report

Campaign: `2026-04-21_10-15-01`  
Git: `master @ fce1833` (dirty files: 5)  
Started: 2026-04-21T08:15:01Z  Finished: 2026-04-21T08:47:38.354Z  
Scenarios: 1, 2, 3, 4, 5, 6, 7  
Runs per scenario: 3  Warmup: 10s  Measure: 60s  
Host: 10 logical CPUs, Darwin 24.5.0  

## Headline: throughput & latency (mean ± stddev over runs)

- `push_msgs/s` = push_rps × push_batch (client-side estimate of items produced).
- `pg_ins/s` = `queen.messages` rows inserted/sec measured from `pg_stat_user_tables` (**PUSH ground truth**).
- `pop_upd/s` = `queen.partition_consumers` rows updated/sec (**POP calls ground truth**; not messages popped — a pop-batch of 100 still counts as one update).
- `pg_tx/s` = `xact_commit` rate across all PG sessions.
- ⚠️ = host was over-subscribed during this run (more heavy threads than CPUs-2). Interpret with caution.

| # | Scenario | push req/s | push msgs/s | pg_ins/s | pop req/s | pop_upd/s | pg_tx/s | push p99 (ms) | errors p/c | host | Δ pg_ins/s vs #1 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|:---:|---:|
| 1 | `pg2c4g_q2w_prod2w100c` | 967 ± 93 | 9667 | 9856 | 858 | 1196 | 580 | 467 ± 111 | 0 / 0 |  | +0.0% |
| 2 | `pg2c4g_q4w_prod4w200c` | 641 ± 68 | 6408 | 6479 | 1557 | 1106 | 714 | 2225 ± 457 | 0 / 0 | ⚠️ | -34.3% |
| 3 | `pg4c8g_q2w_prod4w200c` | 1022 ± 447 | 10222 | 10339 | 1120 | 971 | 665 | 1507 ± 1508 | 0 / 0 |  | +4.9% |
| 4 | `pg4c8g_q4w_prod4w200c` | 280 ± 240 | 2802 | 2846 | 1080 | 503 | 608 | 6072 ± 3289 | 118 / 0 | ⚠️ | -71.1% |
| 5 | `pg4c8g_q4w_prod8w400c` | 161 ± 30 | 1612 | 1682 | 1836 | 206 | 434 | 8979 ± 611 | 1092 / 1 | ⚠️ | -82.9% |
| 6 | `pg4c8g_q8w_prod8w400c` | 151 ± 13 | 1506 | 1710 | 1758 | 239 | 422 | 8810 ± 538 | 1292 / 5 | ⚠️ | -82.7% |
| 7 | `pg4c8g_q8w_prod16w800c` | 170 ± 20 | 1698 | 1741 | 1157 | 82 | 199 | 9768 ± 196 | 2790 / 250 | ⚠️ | -82.3% |

## Per-scenario detail

### Scenario 1 — `pg2c4g_q2w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 966.71 | 92.62 | 862.07 | 1038.19 |
| push_msgs_per_s | 9667.10 | 926.24 | 8620.70 | 10381.90 |
| push_p50 | 79.67 | 6.03 | 74.00 | 86.00 |
| push_p95 | 323.67 | 45.54 | 293.00 | 376.00 |
| push_p99 | 467.33 | 110.62 | 400.00 | 595.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 858.44 | 103.23 | 739.62 | 926.10 |
| pop_p50 | 21.00 | 3.46 | 19.00 | 25.00 |
| pop_p95 | 103.00 | 7.81 | 98.00 | 112.00 |
| pop_p99 | 142.00 | 15.39 | 129.00 | 159.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=0.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 9856 | 824 | 8919 | 10466 |
| partition_consumers updates/s (POP calls) | 1196 | 77 | 1121 | 1275 |
| xact_commit/s | 580 | 28 | 549 | 602 |
| tup_fetched/s | 199503 | 10090 | 189379 | 209559 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 0 | 0 | 0 |
| xact_rollback (delta) | 0 | 0 | 0 | 0 |

Final `queen.messages` rows (mean): 776020 (max 803380).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 198.0, max 198.5
- queen-server CPU%: p95 121.4, max 134.9

### Scenario 2 — `pg2c4g_q4w_prod4w200c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=4, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 640.84 | 67.67 | 591.79 | 718.05 |
| push_msgs_per_s | 6408.43 | 676.75 | 5917.90 | 7180.50 |
| push_p50 | 155.33 | 7.64 | 147.00 | 162.00 |
| push_p95 | 1563.33 | 274.94 | 1246.00 | 1730.00 |
| push_p99 | 2225.33 | 457.10 | 1729.00 | 2629.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1556.81 | 174.08 | 1396.45 | 1741.95 |
| pop_p50 | 15.33 | 1.53 | 14.00 | 17.00 |
| pop_p95 | 148.00 | 20.95 | 125.00 | 166.00 |
| pop_p99 | 239.33 | 57.19 | 185.00 | 299.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=0.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 6479 | 707 | 5882 | 7260 |
| partition_consumers updates/s (POP calls) | 1106 | 220 | 969 | 1360 |
| xact_commit/s | 714 | 50 | 673 | 769 |
| tup_fetched/s | 264834 | 77712 | 175107 | 310662 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 0 | 0 | 0 |
| xact_rollback (delta) | 0 | 0 | 0 | 0 |

Final `queen.messages` rows (mean): 553257 (max 595340).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 213.4, max 232.2
- queen-server CPU%: p95 130.7, max 153.3
- ⚠️ Host over-subscribed: 9 heavy threads on 10 CPUs. Results reflect scheduler contention.

### Scenario 3 — `pg4c8g_q2w_prod4w200c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 1022.22 | 446.76 | 515.64 | 1359.92 |
| push_msgs_per_s | 10222.20 | 4467.58 | 5156.40 | 13599.20 |
| push_p50 | 136.00 | 27.87 | 113.00 | 167.00 |
| push_p95 | 1097.33 | 1041.35 | 459.00 | 2299.00 |
| push_p99 | 1507.00 | 1507.61 | 552.00 | 3245.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1120.28 | 111.57 | 1030.02 | 1245.02 |
| pop_p50 | 28.33 | 7.23 | 20.00 | 33.00 |
| pop_p95 | 180.67 | 25.50 | 155.00 | 206.00 |
| pop_p99 | 259.67 | 55.43 | 211.00 | 320.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=0.0 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 10339 | 4514 | 5229 | 13783 |
| partition_consumers updates/s (POP calls) | 971 | 379 | 547 | 1278 |
| xact_commit/s | 665 | 172 | 504 | 845 |
| tup_fetched/s | 272118 | 105601 | 153057 | 354450 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 0 | 0 | 0 |
| xact_rollback (delta) | 0 | 0 | 0 | 0 |

Final `queen.messages` rows (mean): 861207 (max 1162150).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 413.2, max 456.9
- queen-server CPU%: p95 161.9, max 179.3

### Scenario 4 — `pg4c8g_q4w_prod4w200c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=4, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 280.19 | 239.56 | 72.66 | 542.35 |
| push_msgs_per_s | 2801.87 | 2395.64 | 726.60 | 5423.50 |
| push_p50 | 451.33 | 319.16 | 166.00 | 796.00 |
| push_p95 | 5297.00 | 3381.90 | 2185.00 | 8896.00 |
| push_p99 | 6072.00 | 3289.11 | 2853.00 | 9427.00 |
| push_errors | 117.67 | 180.93 | 0.00 | 326.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1080.48 | 577.93 | 437.65 | 1557.09 |
| pop_p50 | 19.33 | 5.86 | 15.00 | 26.00 |
| pop_p95 | 385.00 | 365.07 | 156.00 | 806.00 |
| pop_p99 | 834.33 | 964.73 | 255.00 | 1948.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=8.0, warnings mean=21.3 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 2846 | 2424 | 792 | 5520 |
| partition_consumers updates/s (POP calls) | 503 | 375 | 174 | 911 |
| xact_commit/s | 608 | 240 | 424 | 880 |
| tup_fetched/s | 126017 | 50513 | 92562 | 184122 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 0 | 0 | 0 |
| xact_rollback (delta) | 7 | 13 | 0 | 22 |

Final `queen.messages` rows (mean): 270600 (max 455310).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 432.2, max 446.8
- queen-server CPU%: p95 123.6, max 175.7
- ⚠️ Host over-subscribed: 9 heavy threads on 10 CPUs. Results reflect scheduler contention.

### Scenario 5 — `pg4c8g_q4w_prod8w400c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=4, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=8, connections=400, push_batch=10
- Consumer: workers=2, connections=100, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 161.18 | 30.42 | 138.69 | 195.80 |
| push_msgs_per_s | 1611.83 | 304.24 | 1386.90 | 1958.00 |
| push_p50 | 280.33 | 109.92 | 182.00 | 399.00 |
| push_p95 | 7701.00 | 966.13 | 7112.00 | 8816.00 |
| push_p99 | 8978.67 | 610.84 | 8581.00 | 9682.00 |
| push_errors | 1092.33 | 278.98 | 775.00 | 1299.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1836.43 | 620.44 | 1463.52 | 2552.64 |
| pop_p50 | 19.00 | 1.73 | 17.00 | 20.00 |
| pop_p95 | 354.33 | 137.09 | 198.00 | 454.00 |
| pop_p99 | 662.33 | 211.80 | 424.00 | 829.00 |
| pop_errors | 1.00 | 1.73 | 0.00 | 3.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=31.3, warnings mean=97.7 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 1682 | 349 | 1442 | 2082 |
| partition_consumers updates/s (POP calls) | 206 | 35 | 185 | 247 |
| xact_commit/s | 434 | 30 | 404 | 464 |
| tup_fetched/s | 123003 | 10802 | 110959 | 131834 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 0 | 0 | 0 |
| xact_rollback (delta) | 31 | 40 | 1 | 76 |

Final `queen.messages` rows (mean): 163633 (max 193790).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 427.8, max 460.5
- queen-server CPU%: p95 116.4, max 135.7
- ⚠️ Host over-subscribed: 14 heavy threads on 10 CPUs. Results reflect scheduler contention.

### Scenario 6 — `pg4c8g_q8w_prod8w400c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=8, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=8, connections=400, push_batch=10
- Consumer: workers=2, connections=100, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 150.60 | 12.87 | 136.27 | 161.17 |
| push_msgs_per_s | 1506.03 | 128.70 | 1362.70 | 1611.70 |
| push_p50 | 277.33 | 30.27 | 245.00 | 305.00 |
| push_p95 | 7617.67 | 685.69 | 7008.00 | 8360.00 |
| push_p99 | 8809.67 | 537.83 | 8283.00 | 9358.00 |
| push_errors | 1292.00 | 26.21 | 1263.00 | 1314.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1758.24 | 121.92 | 1679.37 | 1898.67 |
| pop_p50 | 14.33 | 0.58 | 14.00 | 15.00 |
| pop_p95 | 356.67 | 18.88 | 336.00 | 373.00 |
| pop_p99 | 879.33 | 59.77 | 813.00 | 929.00 |
| pop_errors | 5.00 | 8.66 | 0.00 | 15.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=31.0, warnings mean=112.7 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 1710 | 121 | 1581 | 1821 |
| partition_consumers updates/s (POP calls) | 239 | 26 | 223 | 270 |
| xact_commit/s | 422 | 22 | 396 | 434 |
| tup_fetched/s | 99467 | 9327 | 90881 | 109390 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 0 | 0 | 0 |
| xact_rollback (delta) | 30 | 48 | 0 | 85 |

Final `queen.messages` rows (mean): 140453 (max 151010).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 413.5, max 422.5
- queen-server CPU%: p95 127.1, max 141.9
- ⚠️ Host over-subscribed: 18 heavy threads on 10 CPUs. Results reflect scheduler contention.

### Scenario 7 — `pg4c8g_q8w_prod16w800c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=8, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=16, connections=800, push_batch=10
- Consumer: workers=4, connections=200, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 169.75 | 20.04 | 157.84 | 192.89 |
| push_msgs_per_s | 1697.50 | 200.43 | 1578.40 | 1928.90 |
| push_p50 | 805.67 | 499.92 | 318.00 | 1317.00 |
| push_p95 | 9215.00 | 458.80 | 8804.00 | 9710.00 |
| push_p99 | 9768.00 | 195.50 | 9557.00 | 9943.00 |
| push_errors | 2790.33 | 602.54 | 2096.00 | 3176.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1156.79 | 507.70 | 606.10 | 1606.24 |
| pop_p50 | 20.67 | 3.79 | 18.00 | 25.00 |
| pop_p95 | 1724.33 | 1107.42 | 1073.00 | 3003.00 |
| pop_p99 | 3153.00 | 1600.10 | 2097.00 | 4994.00 |
| pop_errors | 250.00 | 79.73 | 158.00 | 299.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=49.0, warnings mean=292.7 (cosmetic filtered), queue_full hits mean=0.0.

**PG ground truth (diff of `pg_stat_*` over measurement window):**

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| queen.messages inserts/s (PUSH truth) | 1741 | 91 | 1673 | 1844 |
| partition_consumers updates/s (POP calls) | 82 | 20 | 63 | 102 |
| xact_commit/s | 199 | 44 | 149 | 226 |
| tup_fetched/s | 45033 | 10975 | 35876 | 57199 |
| buffer_hit_ratio | 1 | 0 | 1 | 1 |
| deadlocks (delta) | 0 | 0 | 0 | 0 |
| xact_rollback (delta) | 49 | 77 | 0 | 138 |

Final `queen.messages` rows (mean): 168833 (max 183050).

**Resource usage during measurement** (sampled every ~2s):

- PG container CPU%: p95 426.5, max 492.0
- queen-server CPU%: p95 97.6, max 121.8
- ⚠️ Host over-subscribed: 28 heavy threads on 10 CPUs. Results reflect scheduler contention.

## How to read this

1. **Primary signal is `pg_ins/s`** (messages actually inserted, measured from `pg_stat_user_tables`). `push msgs/s` is a client-side estimate and can over-count if retries happen or under-count if the server rejects after acknowledge.
2. **`pg_upd/s` ≈ pops + acks** because autoAck updates message rows to completed. If `pop req/s` is high but `pg_upd/s` is low, the consumer is mostly getting empty pops (queue was drained faster than produced).
3. **Treat deltas as noise** unless `|Δ|` exceeds ~2× the reported stddev. The stddev column tells you how reproducible each number is across the 3 runs.
4. **⚠️ host-oversubscribed rows**: the laptop is running more heavy threads than it has CPUs (minus 2). Those numbers reflect OS scheduler contention more than queen/PG behavior — the workload was bottlenecked on the client-side bench, not the server. Ignore for server-side comparisons.
5. **`push_p99` blow-ups** without matching `pg_ins/s` drop mean the server is queuing but not rejecting — look at `queue_full hits` and the `PG ground truth` section.
6. **High `errors`/`non2xx`** invalidate the run. Re-check server log for reconnects/disconnects.
7. **PG CPU at ~`PG_CPUS × 100%`** means PG is the bottleneck. PG CPU far below that + high push_p99 means queen is the bottleneck.
