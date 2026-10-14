# Perf campaign report

Campaign: `2026-04-20_16-59-25`  
Git: `master @ fce1833` (dirty files: 1)  
Started: 2026-04-20T14:59:25Z  Finished: 2026-04-20T15:29:35.264Z  
Scenarios: 1, 2, 3, 4, 5, 6, 7  
Runs per scenario: 3  Warmup: 10s  Measure: 60s  

## Headline: throughput & latency (mean ± stddev over runs)

`push_msgs/s` = push_rps × push_batch (items actually produced). `Δ vs #1` uses scenario 1 as reference.

| # | Scenario | push req/s | push msgs/s | push p99 (ms) | pop req/s | pop p99 (ms) | errors (p/c) | Δ push msgs/s vs #1 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `pg2c4g_q2w_prod2w100c` | 940 ± 118 | 9396 ± 1182 | 441.7 ± 58.2 | 554 ± 74 | 191.0 ± 6.6 | 0 / 0 | +0.0% |
| 2 | `pg2c4g_q4w_prod4w200c` | 584 ± 67 | 5837 ± 668 | 2302.7 ± 537.1 | 1231 ± 182 | 235.3 ± 45.4 | 0 / 0 | -37.9% |
| 3 | `pg4c8g_q2w_prod4w200c` | 917 ± 231 | 9168 ± 2306 | 1566.0 ± 830.1 | 990 ± 117 | 291.7 ± 45.5 | 23 / 0 | -2.4% |
| 4 | `pg4c8g_q4w_prod4w200c` | 762 ± 53 | 7623 ± 527 | 1701.3 ± 255.2 | 1390 ± 111 | 196.3 ± 21.2 | 0 / 0 | -18.9% |
| 5 | `pg4c8g_q4w_prod8w400c` | 419 ± 68 | 4191 ± 678 | 6718.7 ± 798.5 | 1933 ± 202 | 471.0 ± 128.0 | 286 / 0 | -55.4% |
| 6 | `pg4c8g_q8w_prod8w400c` | 319 ± 30 | 3191 ± 302 | 7488.3 ± 914.9 | 2431 ± 494 | 356.0 ± 78.9 | 652 / 0 | -66.0% |
| 7 | `pg4c8g_q8w_prod16w800c` | 336 ± 98 | 3363 ± 976 | 9213.0 ± 478.6 | 2209 ± 394 | 1293.7 ± 144.7 | 1997 / 35 | -64.2% |

## Per-scenario detail

### Scenario 1 — `pg2c4g_q2w_prod2w100c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=2, connections=100, push_batch=10
- Consumer: workers=1, connections=25, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 939.57 | 118.15 | 854.99 | 1074.57 |
| push_msgs_per_s | 9395.70 | 1181.54 | 8549.90 | 10745.70 |
| push_p50 | 92.33 | 5.51 | 86.00 | 96.00 |
| push_p95 | 299.67 | 18.93 | 278.00 | 313.00 |
| push_p99 | 441.67 | 58.16 | 378.00 | 492.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 554.26 | 74.46 | 502.15 | 639.54 |
| pop_p50 | 22.67 | 4.16 | 18.00 | 26.00 |
| pop_p95 | 122.33 | 14.05 | 109.00 | 137.00 |
| pop_p99 | 191.00 | 6.56 | 184.00 | 197.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=7.0, queue_full hits mean=0.0.

### Scenario 2 — `pg2c4g_q4w_prod4w200c`

- PG: 2c / 4g (max_connections=300)
- Queen: NUM_WORKERS=4, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 583.66 | 66.85 | 517.02 | 650.71 |
| push_msgs_per_s | 5836.60 | 668.46 | 5170.20 | 6507.10 |
| push_p50 | 190.33 | 9.29 | 184.00 | 201.00 |
| push_p95 | 1597.67 | 263.08 | 1300.00 | 1799.00 |
| push_p99 | 2302.67 | 537.06 | 1825.00 | 2884.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1231.02 | 181.91 | 1025.59 | 1371.70 |
| pop_p50 | 11.00 | 2.65 | 9.00 | 14.00 |
| pop_p95 | 183.67 | 15.01 | 169.00 | 199.00 |
| pop_p99 | 235.33 | 45.37 | 202.00 | 287.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=14.3, queue_full hits mean=0.0.

### Scenario 3 — `pg4c8g_q2w_prod4w200c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=2, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 916.81 | 230.57 | 651.87 | 1072.02 |
| push_msgs_per_s | 9168.13 | 2305.71 | 6518.70 | 10720.20 |
| push_p50 | 128.67 | 15.89 | 119.00 | 147.00 |
| push_p95 | 958.33 | 350.72 | 707.00 | 1359.00 |
| push_p99 | 1566.00 | 830.14 | 993.00 | 2518.00 |
| push_errors | 23.00 | 39.84 | 0.00 | 69.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 990.25 | 117.03 | 908.60 | 1124.32 |
| pop_p50 | 28.67 | 3.06 | 26.00 | 32.00 |
| pop_p95 | 199.67 | 25.15 | 171.00 | 218.00 |
| pop_p99 | 291.67 | 45.50 | 246.00 | 337.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=9.3, queue_full hits mean=0.0.

### Scenario 4 — `pg4c8g_q4w_prod4w200c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=4, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=4, connections=200, push_batch=10
- Consumer: workers=1, connections=50, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 762.25 | 52.68 | 701.49 | 795.00 |
| push_msgs_per_s | 7622.53 | 526.76 | 7014.90 | 7950.00 |
| push_p50 | 135.33 | 0.58 | 135.00 | 136.00 |
| push_p95 | 1274.00 | 211.54 | 1119.00 | 1515.00 |
| push_p99 | 1701.33 | 255.20 | 1514.00 | 1992.00 |
| push_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1390.45 | 111.40 | 1271.12 | 1491.72 |
| pop_p50 | 20.00 | 3.61 | 16.00 | 23.00 |
| pop_p95 | 126.67 | 11.06 | 115.00 | 137.00 |
| pop_p99 | 196.33 | 21.22 | 172.00 | 211.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=13.0, queue_full hits mean=0.0.

### Scenario 5 — `pg4c8g_q4w_prod8w400c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=4, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=8, connections=400, push_batch=10
- Consumer: workers=2, connections=100, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 419.11 | 67.77 | 357.45 | 491.67 |
| push_msgs_per_s | 4191.07 | 677.71 | 3574.50 | 4916.70 |
| push_p50 | 186.33 | 10.50 | 176.00 | 197.00 |
| push_p95 | 5257.00 | 802.29 | 4435.00 | 6038.00 |
| push_p99 | 6718.67 | 798.53 | 6003.00 | 7580.00 |
| push_errors | 286.33 | 199.76 | 81.00 | 480.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 1932.89 | 202.08 | 1713.59 | 2111.59 |
| pop_p50 | 17.00 | 1.73 | 16.00 | 19.00 |
| pop_p95 | 247.33 | 44.64 | 196.00 | 277.00 |
| pop_p99 | 471.00 | 128.04 | 328.00 | 575.00 |
| pop_errors | 0.33 | 0.58 | 0.00 | 1.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=0.0, warnings mean=13.0, queue_full hits mean=0.0.

### Scenario 6 — `pg4c8g_q8w_prod8w400c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=8, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=8, connections=400, push_batch=10
- Consumer: workers=2, connections=100, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 319.12 | 30.19 | 287.49 | 347.62 |
| push_msgs_per_s | 3191.17 | 301.86 | 2874.90 | 3476.20 |
| push_p50 | 219.00 | 68.64 | 174.00 | 298.00 |
| push_p95 | 5831.00 | 872.55 | 4827.00 | 6406.00 |
| push_p99 | 7488.33 | 914.92 | 6437.00 | 8104.00 |
| push_errors | 652.33 | 152.04 | 485.00 | 782.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 2430.72 | 494.16 | 1873.17 | 2814.59 |
| pop_p50 | 11.67 | 1.15 | 11.00 | 13.00 |
| pop_p95 | 222.67 | 52.31 | 190.00 | 283.00 |
| pop_p99 | 356.00 | 78.86 | 299.00 | 446.00 |
| pop_errors | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=3.0, warnings mean=35.3, queue_full hits mean=0.0.

### Scenario 7 — `pg4c8g_q8w_prod16w800c`

- PG: 4c / 8g (max_connections=300)
- Queen: NUM_WORKERS=8, SIDECAR_POOL_SIZE=200 (total), DB_POOL_SIZE=20 (total)
- Producer: workers=16, connections=800, push_batch=10
- Consumer: workers=4, connections=200, pop_batch=100

| metric | mean | stddev | min | max |
|---|---:|---:|---:|---:|
| push_rps | 336.25 | 97.60 | 247.57 | 440.82 |
| push_msgs_per_s | 3362.50 | 976.00 | 2475.70 | 4408.20 |
| push_p50 | 223.67 | 60.29 | 178.00 | 292.00 |
| push_p95 | 8336.67 | 782.80 | 7660.00 | 9194.00 |
| push_p99 | 9213.00 | 478.61 | 8866.00 | 9759.00 |
| push_errors | 1997.00 | 242.69 | 1797.00 | 2267.00 |
| push_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |
| pop_rps | 2208.58 | 393.59 | 1848.64 | 2628.85 |
| pop_p50 | 16.33 | 2.08 | 14.00 | 18.00 |
| pop_p95 | 621.67 | 81.83 | 529.00 | 684.00 |
| pop_p99 | 1293.67 | 144.68 | 1197.00 | 1460.00 |
| pop_errors | 34.67 | 32.33 | 0.00 | 64.00 |
| pop_non2xx | 0.00 | 0.00 | 0.00 | 0.00 |

Server log: lag events mean=0.0 (max 0ms), error lines mean=22.3, warnings mean=149.3, queue_full hits mean=0.0.

## How to read this

1. **Look first at `push msgs/s` with `Δ vs #1`** — that is items/s actually enqueued. `push req/s` is HTTP requests/s.
2. **Treat deltas as noise** unless `|Δ|` exceeds ~2× the reported stddev.
3. **`push_p99` blow-ups** without matching `push msgs/s` drop usually mean the server is queuing but not rejecting — look at `queue_full hits`.
4. **High `errors`/`non2xx`** invalidate the run. Re-check server log for reconnects or disconnects.
5. **`pop_p99` stays low at high load** only if the consumer keeps up. If `pop` falls far behind `push`, the queue backs up (visible in `queue_final_total` in `aggregate.json`).
