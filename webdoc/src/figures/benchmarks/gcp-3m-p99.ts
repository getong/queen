import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "3,000,000 msg/s": p99 at 3M msg/s of 256-byte messages, 2026-10-14, three GCP
// c4-standard-24-lssd brokers. The slowest load process's worst 10 s window, seconds 30 to 60, on both sides.
// Kafka with vm.dirty_background_bytes = 1 GiB (its better setting, see the page). Kafka at 100,000 partitions did
// not carry the rate (200k msg/s pushed, p99 51 s) and is left off the linear axis; 1M and 10M were not run.
export default line({
  alt: "p99 end-to-end latency at 3,000,000 msg/s against the partition count (log scale), Queen 2.3.0 and Kafka 4.3.1 on the same three GCP nodes. Queen reads 78 ms at 10 partitions, 65 at 100, 56 at 1,000, 66 at 10,000, 71 at 100,000, 47 at 1,000,000 and 54 at 10,000,000. Kafka reads 514 ms at 10 partitions, 61 at 100, 104 at 1,000 and 110 at 10,000, and does not carry 100,000.",
  caption: "At 3M msg/s Queen's p99 stays between 47 and 78 ms from 10 to 10M partitions. Kafka matches it at 100 partitions, is about twice as high at 1,000 and 10,000, and does not carry 100,000 (not drawn).",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs, the m3-* folders (Queen); benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the *-3000k-gcp24-dbb1g folders (Kafka)",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 100, 200, 300, 400, 500], min: 0, max: 550 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[10, 78], [100, 65], [1000, 56], [10000, 66], [100000, 71], [1000000, 47], [10000000, 54]] },
    { label: "Kafka 4.3.1", points: [[10, 514], [100, 61], [1000, 104], [10000, 110]] },
  ],
});
