import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "1,000,000 msg/s of 1 KB messages": p99 at 1M msg/s of 1 KB JSON messages,
// 2026-10-14, the same three GCP brokers. The median of the three windows in seconds 30 to 60 (each window: the slowest
// of nine load processes). Kafka on XFS with thread pools for 24 vCPU and writeback from 1 GiB. Kafka at 50,000 and
// 100,000 partitions did not carry the rate (159k and 153k msg/s pushed, p99 33 s and 35 s) and is left off the linear
// axis; 1M and 10M were not run. Queen was not run at 25,000 and 50,000.
export default line({
  alt: "p99 end-to-end latency at 1,000,000 msg/s of 1 KB messages against the partition count (log scale), Queen 2.3.0 and Kafka 4.3.1 on the same three GCP nodes, the median of three 10-second windows. Queen reads 47 ms at 10 partitions, 30 at 100, 26 at 1,000, 28 at 10,000, 27 at 100,000, 26 at 1,000,000 and 28 at 10,000,000. Kafka on XFS reads 9.4 ms at 10, 100 and 1,000 partitions, 19 at 10,000 and 45 at 25,000, and does not carry 50,000 or 100,000.",
  caption: "With 1 KB messages at 1M msg/s Kafka's p99 is 9.4 to 19 ms up to 10,000 partitions and 45 ms at 25,000; it does not carry 50,000 or 100,000 (not drawn). Queen's is 26 to 30 ms from 100 to 10M partitions.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs, the p1k-* folders (Queen); benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the *-1000k-p1k-gcp24-xfs-t24 folders (Kafka)",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 10, 20, 30, 40, 50], min: 0, max: 54 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[10, 47], [100, 30], [1000, 26], [10000, 28], [100000, 27], [1000000, 26], [10000000, 28]] },
    { label: "Kafka 4.3.1", points: [[10, 9.4], [100, 9.4], [1000, 9.4], [10000, 19], [25000, 45]] },
  ],
});
