import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "1,000,000 msg/s of 1 KB messages": CPU of the three brokers summed (of 72 vCPU)
// over the loaded window, 2026-10-14. Hollow = not carried (Kafka at 50,000 and 100,000 partitions). Kafka on XFS with
// thread pools for 24 vCPU and writeback from 1 GiB.
export default line({
  alt: "Cores used by the three brokers together at 1,000,000 msg/s of 1 KB messages against the partition count (log scale). Queen uses 13.8 cores at 10 partitions and 16.0 to 17.4 from 100 to 10,000,000. Kafka uses 9.2 at 10, 9.5 at 100, 11.9 at 1,000, 31.4 at 10,000 and 55.9 at 25,000, then 46.5 at 50,000 and 60.6 at 100,000, where it did not carry the rate.",
  caption: "With 1 KB messages Kafka uses less CPU than Queen up to 1,000 partitions, about twice Queen's at 10,000, 56 cores at 25,000, and fails at 50,000 and 100,000 (hollow). Queen stays at 16 to 17 cores up to ten million.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs/matrix-gcp24-1m1k.json (Queen); report/report.py over the *-1000k-p1k-gcp24-xfs-t24 Kafka runs",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "cores, three brokers together (of 72)", ticks: [0, 20, 40, 60], min: 0, max: 64 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[10, 13.8], [100, 17.4], [1000, 16.1], [10000, 16.2], [100000, 16.5], [1000000, 16.0], [10000000, 16.5]] },
    { label: "Kafka 4.3.1", points: [[10, 9.2], [100, 9.5], [1000, 11.9], [10000, 31.4], [25000, 55.9], { x: 50000, y: 46.5, hollow: true }, { x: 100000, y: 60.6, hollow: true }] },
  ],
});
