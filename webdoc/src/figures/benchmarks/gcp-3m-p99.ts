import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "3,000,000 msg/s": p99 at 3M msg/s of 256-byte messages, 2026-10-14, three GCP
// c4-standard-24-lssd brokers.
// The median of the three windows in seconds 30 to 60 (each window: the slowest of nine load processes). Kafka on XFS
// with 24 request handler, 12 network and 12 replica fetcher threads and writeback from 1 GiB (see the page). Kafka at
// 50,000 and 100,000 partitions did not carry the rate (601k and 174k msg/s pushed, p99 32 s and 38 s) and is left off
// the linear axis; 1M and 10M were not run. Queen was not run at 25,000 and 50,000.
export default line({
  alt: "p99 end-to-end latency at 3,000,000 msg/s against the partition count (log scale), Queen 2.3.0 and Kafka 4.3.1 on the same three GCP nodes, the median of three 10-second windows. Queen reads 76 ms at 10 partitions, 54 at 100, 47 at 1,000, 61 at 10,000, 53 at 100,000, 42 at 1,000,000 and 46 at 10,000,000. Kafka on XFS reads 11 ms at 10 partitions, 7.5 at 100, 8.1 at 1,000, 21 at 10,000 and 105 at 25,000, and does not carry 50,000 or 100,000.",
  caption: "At 3M msg/s Kafka's p99 is 7.5 to 21 ms up to 10,000 partitions, below Queen's 47 to 76, and 105 ms at 25,000; it does not carry 50,000 or 100,000 (not drawn). Queen stays between 42 and 76 ms from 10 to 10M partitions.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs, the m3-* folders (Queen); benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the *-3000k-gcp24-xfs-t24 folders (Kafka)",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 20, 40, 60, 80, 100], min: 0, max: 112 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[10, 76], [100, 54], [1000, 47], [10000, 61], [100000, 53], [1000000, 42], [10000000, 46]] },
    { label: "Kafka 4.3.1", points: [[10, 11], [100, 7.5], [1000, 8.1], [10000, 21], [25000, 105]] },
  ],
});
