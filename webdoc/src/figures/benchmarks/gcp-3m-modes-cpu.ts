import { line } from "@/lib/figure-spec";

// Broker CPU of the same runs as gcp-3m-modes-p99.ts: the three brokers summed (of 72 vCPU) over the loaded window.
// Hollow = not carried (Kafka streaming at 50,000 and 100,000 partitions). benchmarks/queen-vs-kafka.mdx, "Kafka in queue mode".
export default line({
  alt: "Cores used by the three brokers together at 3,000,000 msg/s against the partition count (log scale). Queen uses 18.6 cores at 10 partitions and 22.4 to 23.4 from 100 to 10,000,000. Kafka with a consumer group uses 14.9 at 10, 18.5 at 100, 21.0 at 1,000, 52.8 at 10,000 and 57.0 at 25,000, and does not carry 50,000 or 100,000. Kafka with a share group uses 24.6 at 100, 35.0 at 1,000 and 52.2 at 10,000.",
  caption: "Queue mode costs Kafka cores: 6 more than streaming at 100 partitions and 14 more at 1,000, 50% above Queen there. Queen's CPU does not move with the partition count.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs/matrix-gcp24-3m.json (Queen); report/report.py over the Kafka folders above",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "cores, three brokers together (of 72)", ticks: [0, 20, 40, 60], min: 0, max: 64 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[10, 18.6], [100, 23.0], [1000, 22.4], [10000, 23.3], [100000, 23.4], [1000000, 23.0], [10000000, 23.4]] },
    { label: "Kafka, consumer group (streaming)", tone: "s2", points: [[10, 14.9], [100, 18.5], [1000, 21.0], [10000, 52.8], [25000, 57.0], { x: 50000, y: 50.2, hollow: true }, { x: 100000, y: 60.5, hollow: true }] },
    { label: "Kafka, share group (queue mode)", tone: "s3", points: [[100, 24.6], [1000, 35.0], [10000, 52.2]] },
  ],
});
