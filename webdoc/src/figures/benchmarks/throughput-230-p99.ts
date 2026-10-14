import { line } from "@/lib/figure-spec";

// benchmarks/throughput.mdx, "Latency by rate": the p99 halves of the table,
// Queen 2.3.0 pre-release builds and Kafka 4.3.1 on the same machines,
// 2026-10-12 and 13. Each line stops at the last rate the system carried.
export default line({
  alt: "p99 end-to-end latency against the offered rate, one queue or topic of 100 partitions on three nodes. Queen 2.3.0 goes from 6 ms at 100,000 msg/s to 9 ms at 500,000, 19 ms at 1,000,000, 41 ms at 1,500,000 and 63 ms at 2,000,000. Kafka reads 11 ms at 1,000,000, 43 ms at 2,000,000 and 243 ms at 4,000,000.",
  caption: "Each line stops at the last rate the system carried. On these 16-vCPU nodes Kafka is ahead at every rate both ran and goes on to 4M msg/s; one Queen raft group stops between 2M and 2.5M, at its planner.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms (Queen), benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the -oct13 folders (Kafka)",
  x: { label: "offered msg/s", ticks: [0, 1000000, 2000000, 3000000, 4000000], min: 0, max: 4200000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 50, 100, 150, 200, 250], min: 0, max: 260 },
  series: [
    { label: "Queen 2.3.0", points: [[100000, 6], [500000, 9], [1000000, 19], [1500000, 41], [2000000, 63]] },
    { label: "Kafka 4.3.1", points: [[1000000, 11], [2000000, 43], [4000000, 243]] },
  ],
});
