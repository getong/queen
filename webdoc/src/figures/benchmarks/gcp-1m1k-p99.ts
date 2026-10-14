import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "1,000,000 msg/s of 1 KB messages": p99 at 1M msg/s of 1 KB JSON messages,
// 2026-10-14, the same three GCP brokers. The slowest load process's worst 10 s window, seconds 30 to 60. Kafka with
// dirty_background_bytes = 1 GiB. Kafka at 100,000 partitions did not carry the rate (166k msg/s pushed, p99 44 s) and
// is left off the linear axis; 1M and 10M were not run.
export default line({
  alt: "p99 end-to-end latency at 1,000,000 msg/s of 1 KB messages against the partition count (log scale), Queen 2.3.0 and Kafka 4.3.1 on the same three GCP nodes. Queen reads 68 ms at 10 partitions, 34 at 100, 27 at 1,000, 28 at 10,000, 27 at 100,000, 29 at 1,000,000 and 65 at 10,000,000. Kafka reads 553 ms at 10 partitions, 136 at 100, 98 at 1,000 and 132 at 10,000, and does not carry 100,000.",
  caption: "With 1 KB messages at 1M msg/s Queen's p99 is 27 to 34 ms from 100 to 1M partitions. Kafka's is 98 to 136 ms, and it does not carry 100,000 partitions (not drawn).",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs, the p1k-* folders (Queen); benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the *-1000k-p1k-gcp24-dbb1g folders (Kafka)",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 100, 200, 300, 400, 500], min: 0, max: 580 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[10, 68], [100, 34], [1000, 27], [10000, 28], [100000, 27], [1000000, 29], [10000000, 65]] },
    { label: "Kafka 4.3.1", points: [[10, 553], [100, 136], [1000, 98], [10000, 132]] },
  ],
});
