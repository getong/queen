import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "The ceiling at 1,000 partitions": p99 against the offered rate on one queue or topic
// of 1,000 partitions, 2026-10-14, the same three GCP brokers, a fresh cluster for each rate. The median of the three
// windows in seconds 30 to 60. Queen at 4M was not carried (3.95M pushed, p99 1.2 s) and Kafka at 10M read 375 ms
// (carried by the rule, the disks at their limit): both are off the axis and named in the caption.
export default line({
  alt: "p99 end-to-end latency against the offered rate on 1,000 partitions, Queen 2.3.0 and Kafka 4.3.1 on the same three GCP nodes, the median of three 10-second windows. Queen reads 17 ms at 2,000,000 msg/s, 27 at 2,500,000, 50 at 3,000,000 and 97 at 3,500,000, and does not carry 4,000,000. Kafka on XFS reads 8.1 ms at 3,000,000, 8.3 at 4,000,000, 8.8 at 5,000,000, 10.8 at 6,000,000, 10.3 at 7,000,000 and 12.5 at 8,000,000, and 375 ms at 10,000,000.",
  caption: "On 1,000 partitions Kafka's p99 stays between 8 and 13 ms from 3M to 8M msg/s; at 10M it reads 375 ms (not drawn). Queen's rises from 17 ms at 2M to 97 ms at 3.5M, and 4M is not carried.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs, the gcp24-* ceiling folders (Queen); benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the 1x1000-<rate>k-gcp24-xfs-t24 folders (Kafka)",
  x: { label: "offered msg/s", ticks: [2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000], min: 1800000, max: 8300000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 20, 40, 60, 80, 100], min: 0, max: 110 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[2000000, 17], [2500000, 27], [3000000, 50], [3500000, 97]] },
    { label: "Kafka 4.3.1", points: [[3000000, 8.1], [4000000, 8.3], [5000000, 8.8], [6000000, 10.8], [7000000, 10.3], [8000000, 12.5]] },
  ],
});
