import { line } from "@/lib/figure-spec";

// benchmarks/partitions.mdx, "Kafka on the same machines": p99 at 2,000,000
// msg/s, 2026-10-13. Hollow = not carried (Kafka at 100,000 partitions: 1.67M
// consumed of 2M, p99 15 s and rising). Kafka was not run past that point.
export default line({
  alt: "p99 end-to-end latency at 2,000,000 msg/s against the partition count, both on log scales, Queen 2.3.0 and Kafka 4.3.1 on the same three nodes. Queen reads 100 ms at 10 partitions and stays between 59 and 73 ms from 100 to 10,000,000. Kafka reads 403 ms at 10 partitions, 43 ms at 100, 25 ms at 1,000 and 134 ms at 10,000, and does not carry 100,000, where its p99 was 15 seconds and rising.",
  caption: "At 2M msg/s Kafka is ahead up to 1,000 partitions and behind from 10,000. The hollow mark is the point Kafka did not carry; it was not run past it.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms (Queen), benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the -oct13 folders (Kafka)",
  x: { label: "partitions in the queue", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", log: true, format: "ms", ticks: [10, 100, 1000, 10000], min: 10, max: 30000 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[10, 100], [100, 63], [1000, 70], [10000, 63], [100000, 59], [1000000, 66], [10000000, 73]] },
    { label: "Kafka 4.3.1", points: [[10, 403], [100, 43], [1000, 25], [10000, 134], { x: 100000, y: 15100, hollow: true }] },
  ],
});
