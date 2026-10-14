# 1M msg/s of 1 KB messages by partitions, GCP c4-standard-24-lssd x3 (RAID0 of 4 local NVMe), Queen 2.3.0, 2026-10-14

Fresh cluster a point, 100 msg a request, 1 KB JSON (1,027 B), dedup off, completed kept 1,800 s, binary push + zstd + counts ack + framed pop.
Latency: last 30 s of a 70 s step (harness rows). Disk/net: mean of the last 30 s of load from /sys/block/md0 and the private NIC, per node n1 (leader) / n2 / n3; ms a write = median of the per-second means. Memory: broker RSS max and machine free memory min around that window.

| partitions | carried | push p50 / p99 | e2e p50 / p99 | cores n1/n2/n3 | broker RSS GB | free mem min GB | disk MB/s | ms a write (max) | disk busy % | leader rx / tx MB/s | loaders cores |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | yes | 5.1 / 13.7 | 15.2 / 45.1 | 7.3 / 3.3 / 3.2 | 4.7 / 4.2 / 4.3 | 27 / 25 / 25 | 665 / 665 / 670 | 0.18 (0.2) / 0.09 (0.1) / 0.09 (0.1) | 55 / 38 / 38 | 1361 / 1227 | 15.0 |
| 100 | yes | 5.1 / 12.4 | 9.3 / 27.9 | 9.1 / 4.1 / 4.2 | 5.6 / 5.1 / 5.2 | 26 / 26 / 26 | 681 / 674 / 675 | 0.10 (0.1) / 0.09 (0.1) / 0.09 (0.1) | 45 / 39 / 39 | 1367 / 1237 | 17.4 |
| 1,000 | yes | 4.8 / 12.0 | 9.3 / 24.8 | 8.5 / 3.8 / 3.8 | 6.5 / 5.4 / 5.2 | 60 / 60 / 60 | 720 / 709 / 706 | 0.10 (0.1) / 0.12 (0.1) / 0.12 (0.2) | 45 / 40 / 41 | 1363 / 1229 | 15.7 |
| 10,000 | yes | 5.1 / 13.8 | 10.1 / 27.6 | 8.5 / 3.8 / 3.9 | 7.0 / 5.5 / 5.4 | 25 / 26 / 26 | 732 / 724 / 725 | 0.17 (0.2) / 0.13 (0.2) / 0.12 (0.2) | 52 / 43 / 44 | 1363 / 1229 | 15.6 |
| 100,000 | yes | 5.2 / 13.1 | 10.2 / 26.1 | 8.7 / 3.9 / 3.9 | 7.5 / 5.5 / 5.5 | 24 / 25 / 25 | 724 / 712 / 713 | 0.12 (0.2) / 0.13 (0.2) / 0.12 (0.2) | 46 / 44 / 41 | 1363 / 1227 | 15.7 |
| 1,000,000 | yes | 5.3 / 12.8 | 10.8 / 25.3 | 8.6 / 3.7 / 3.7 | 8.9 / 5.8 / 5.8 | 20 / 23 / 24 | 680 / 671 / 672 | 0.08 (0.1) / 0.09 (0.1) / 0.09 (0.1) | 43 / 37 / 37 | 1363 / 1227 | 15.9 |
| 10,000,000 | yes | 5.1 / 13.8 | 10.0 / 27.6 | 8.8 / 3.8 / 3.9 | 18.3 / 15.5 / 15.5 | 4 / 3 / 3 | 681 / 672 / 673 | 0.08 (0.1) / 0.09 (0.1) / 0.09 (0.2) | 41 / 38 / 37 | 1365 / 1230 | 16.0 |

Leader threads (cores, last 30 s): 
- 10: tokio-rt-worker 2.61, queen-raft 1.92, queen-qlog-writ 0.31, queen-rsm-apply 0.16
- 100: tokio-rt-worker 3.54, queen-raft 2.10, queen-qlog-writ 0.32, queen-rsm-apply 0.29
- 1,000: tokio-rt-worker 3.04, queen-raft 2.05, queen-qlog-writ 0.32, queen-rsm-apply 0.31
- 10,000: tokio-rt-worker 3.05, queen-raft 2.01, queen-qlog-writ 0.31, queen-rsm-apply 0.31
- 100,000: tokio-rt-worker 3.08, queen-raft 2.02, queen-rsm-apply 0.32, queen-qlog-writ 0.32
- 1,000,000: tokio-rt-worker 3.09, queen-raft 2.00, queen-rsm-apply 0.32, queen-qlog-writ 0.32
- 10,000,000: tokio-rt-worker 3.18, queen-raft 2.01, queen-rsm-apply 0.33, queen-qlog-writ 0.32

Per 10 s e2e p99 (ms): 
- 10: [31, 48, 44, 62, 45, 45, 43]
- 100: [22, 33, 32, 32, 26, 30, 26]
- 1,000: [26, 26, 25, 24, 24, 26, 24]
- 10,000: [23, 29, 26, 27, 26, 26, 31]
- 100,000: [23, 29, 27, 26, 26, 25, 27]
- 1,000,000: [22, 28, 30, 27, 25, 23, 26]
- 10,000,000: [27, 28, 30, 26, 28, 26, 27]

Raw: runs/<dir>/ (net-n*.log per second: disk + NIC; sample-n*.log per 10 s: memory + CPU per thread; q*.json loaders; broker-n*.log.gz).
