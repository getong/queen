import { line } from "@/lib/figure-spec";

// benchmarks/throughput.mdx, "CPU": cores used by each system, summed over the
// three brokers (48 vCPU in all), 100 partitions, 2026-10-12 and 13.
export default line({
  alt: "Cores used by each system against the offered rate, summed over three brokers with 48 vCPU in all, one queue or topic of 100 partitions. Queen 2.3.0 used 4.6 cores at 100,000 msg/s, 11.3 at 500,000, 15.1 at 1,000,000, 17.8 at 1,500,000 and 19.9 at 2,000,000. Kafka used 9.8 cores at 1,000,000, 15.8 at 2,000,000 and 21.8 at 4,000,000.",
  caption: "With few partitions Queen spends one and a half times Kafka's CPU for a message at 1M msg/s and one and a quarter at 2M.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms (Queen), benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the -oct13 folders (Kafka)",
  x: { label: "offered msg/s", ticks: [0, 1000000, 2000000, 3000000, 4000000], min: 0, max: 4200000 },
  y: { label: "cores, three brokers", ticks: [0, 10, 20, 30], min: 0, max: 30, format: "int" },
  series: [
    { label: "Queen 2.3.0", points: [[100000, 4.6], [500000, 11.3], [1000000, 15.1], [1500000, 17.8], [2000000, 19.9]] },
    { label: "Kafka 4.3.1", points: [[1000000, 9.8], [2000000, 15.8], [4000000, 21.8]] },
  ],
});
