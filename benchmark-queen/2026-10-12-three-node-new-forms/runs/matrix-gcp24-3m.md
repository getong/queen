# 3M msg/s by partitions, GCP c4-standard-24-lssd x3 (RAID0 of 4 local NVMe), Queen 2.3.0, 2026-10-14

Fresh cluster a point, 100 msg a request, 256 B JSON, dedup off, completed kept 1,800 s, binary push + zstd + counts ack + framed pop.
Latency: last 30 s of a 70 s step (harness rows). Disk/net: mean of the last 30 s of load from /sys/block/md0 and the private NIC, per node n1 (leader) / n2 / n3; ms a write = median of the per-second means. Memory: broker RSS max and machine free memory min around that window.

| partitions | carried | push p50 / p99 | e2e p50 / p99 | cores n1/n2/n3 | broker RSS GB | free mem min GB | disk MB/s | ms a write (max) | disk busy % | leader rx / tx MB/s | loaders cores |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | yes | 8.2 / 19.5 | 36.4 / 70.7 | 9.4 / 4.6 / 4.6 | 11.1 / 10.6 / 10.6 | 52 / 53 / 52 | 716 / 711 / 713 | 0.49 (0.8) / 0.55 (0.7) / 0.53 (0.7) | 36 / 34 / 35 | 1359 / 899 | 17.9 |
| 100 | yes | 7.4 / 23.6 | 21.0 / 53.8 | 11.5 / 5.7 / 5.8 | 13.3 / 12.9 / 12.8 | 52 / 52 / 52 | 728 / 724 / 723 | 0.42 (0.5) / 0.50 (0.6) / 0.50 (0.6) | 38 / 37 / 37 | 1366 / 902 | 20.1 |
| 1,000 | yes | 8.8 / 22.0 | 20.5 / 44.0 | 11.3 / 5.5 / 5.6 | 14.8 / 14.3 / 14.3 | 51 / 53 / 53 | 764 / 759 / 758 | 0.42 (0.5) / 0.48 (0.8) / 0.47 (0.6) | 39 / 38 / 38 | 1367 / 902 | 19.0 |
| 10,000 | yes | 10.9 / 30.7 | 24.1 / 53.8 | 11.7 / 5.8 / 5.8 | 15.2 / 14.7 / 14.6 | 51 / 52 / 52 | 959 / 957 / 957 | 0.34 (1.1) / 0.38 (0.6) / 0.35 (0.5) | 56 / 57 / 55 | 1369 / 902 | 19.0 |
| 100,000 | NO | 10.2 / 30.2 | 23.0 / 52.7 | 11.7 / 5.8 / 5.9 | 16.0 / 14.7 / 14.7 | 50 / 52 / 52 | 880 / 876 / 877 | 0.29 (0.4) / 0.34 (0.9) / 0.30 (0.4) | 48 / 49 / 48 | 1370 / 904 | 19.0 |
| 1,000,000 | yes | 10.5 / 23.3 | 23.8 / 42.5 | 11.8 / 5.6 / 5.6 | 15.9 / 13.9 / 13.9 | 48 / 51 / 51 | 790 / 787 / 788 | 0.44 (0.5) / 0.48 (0.6) / 0.46 (0.6) | 41 / 40 / 40 | 1372 / 902 | 18.9 |
| 10,000,000 | yes | 10.4 / 25.9 | 23.6 / 47.1 | 12.0 / 5.6 / 5.8 | 25.4 / 22.5 / 22.5 | 27 / 30 / 31 | 736 / 733 / 732 | 0.46 (0.9) / 0.52 (0.8) / 0.56 (0.7) | 35 / 35 / 34 | 1376 / 905 | 18.9 |

Leader threads (cores, last 30 s): 
- 10: tokio-rt-worker 4.10, queen-raft 1.64, queen-rsm-apply 0.35, queen-qlog-writ 0.23
- 100: tokio-rt-worker 5.32, queen-raft 1.81, queen-rsm-apply 0.47, queen-qlog-writ 0.25
- 1,000: tokio-rt-worker 4.91, queen-raft 1.73, queen-rsm-apply 0.61, queen-qlog-writ 0.24
- 10,000: tokio-rt-worker 5.01, queen-raft 1.69, queen-rsm-apply 0.64, queen-rsm-ckpt 0.47
- 100,000: tokio-rt-worker 5.01, queen-raft 1.71, queen-rsm-apply 0.66, queen-rsm-ckpt 0.41
- 1,000,000: tokio-rt-worker 5.06, queen-raft 1.69, queen-rsm-apply 0.66, queen-rsm-ckpt 0.28
- 10,000,000: tokio-rt-worker 5.20, queen-raft 1.71, queen-rsm-apply 0.66, queen-lane-1 0.37

Per 10 s e2e p99 (ms): 
- 10: [56, 118, 95, 72, 73, 72, 66]
- 100: [40, 87, 60, 52, 63, 43, 52]
- 1,000: [49, 47, 54, 50, 46, 44, 37]
- 10,000: [78, 65, 66, 64, 60, 57, 46]
- 100,000: [36, 55, 48, 50, 44, 66, 49]
- 1,000,000: [35, 51, 39, 41, 46, 39, 41]
- 10,000,000: [31, 47, 53, 43, 44, 50, 47]

Raw: runs/<dir>/ (net-n*.log per second: disk + NIC; sample-n*.log per 10 s: memory + CPU per thread; q*.json loaders; broker-n*.log.gz).

Notes:
- 100,000 partitions is "NO" by the rule (nothing may err) for ONE pop: process q8 (loader 3, broker n3) got HTTP 503
  `{"error":"the request deadline elapsed","code":"timeout"}` at 09:59:54.7Z, in the first seconds of the load. Pushed,
  consumed, shed and latency are like the carried points. Cause not looked into; n3's log has nothing at that time.
- 10,000,000 partitions ran with `QUEEN_CONSUME_IDLE_UNLOAD_S=30` and a 240 s drain of the 10M prefill before the step
  (as DO's 10M rows): the warm step printed "FATAL warm drain bench: 0 of 10000000 messages came out" and the drain
  then consumed 9,997,440 with 594 errors (DO: 891), the same sequence as DO's 10M runs; the measured step had 0 errors.
- 100,000 ran last (10:00Z), after 10,000,000, on its own launcher (added to the list after the start).
