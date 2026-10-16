# Launchers of 2026-10-16 (GCP, Pulsar and Redpanda next to Queen 2.4)

The scripts that ran the points of `runs/pulsar/1x*-3000k-gcp24*`, `runs/redpanda/1x*-3000k-gcp24`,
`runs/kafka/1x1000-3000k-gcp24-xfs-t24-1016` and Queen's `../2026-10-12-three-node-new-forms/runs/a24-*`, in the order
they ran. `S` was a scratch directory (markers, logs); the brokers' public addresses are placeholders.

| script | what |
|---|---|
| `chain.sh` | Queen 2.4 and Kafka anchors at 3M on 1,000 partitions, then Pulsar and Redpanda at 10, 100, 1k, 10k |
| `chain2.sh` | reruns of the points lost to ssh timeouts from the operator's machine (Pulsar 1k and 10k, Redpanda 10) |
| `chain3.sh` | 50k and 100k: Queen 50k, Pulsar and Redpanda 50k (100k only after a carried 50k) |
| `chain4.sh` | Pulsar 50k with 8 connections per broker (`-c8`) |
| `chain5.sh` to `chain8.sh` | Pulsar on six load generators (`hosts-6l.env`): 50k with a 1 s batch delay (`-6l`), with default batching (`-6l-b5`), 100k without the warm-up (`-nowarm-r`) |
| `next.sh` | waits for the next point to end (progress reports) |
| `sum1016.py`, `full1016.py` | the docs statistic for every run (median of the three windows of seconds 30-60, slowest process), one table |

The folders marked `ABORTED` or `INVALID` are kept for what they show: `-6l-ABORTED-batchdelay1s` (100k with the 1 s
batch delay, stopped: the latency was the delay), `-6l-b5-ABORTED-warm25min` (the one-process warm-up at 100k still
running after 25 minutes; `create.txt.excerpt` is the head and tail of its 45 MB log), and
`-6l-b5-nowarm-INVALID-stray-create-on-l1` (a `pload -create-only` of the aborted run ignored SIGTERM and kept its
100,000 producers on load generator 1 through this run). Heavy files (Pulsar's `logs.tgz`, Redpanda's and Kafka's
`*.gz` logs, Kafka's GC log) are not in git.
