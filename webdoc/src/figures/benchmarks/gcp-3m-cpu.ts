import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "3,000,000 msg/s": CPU of the three brokers summed (of 72 vCPU) over the loaded
// window, 2026-10-14. Hollow = not carried (Kafka at 50,000 and 100,000 partitions). Kafka on XFS with thread pools for
// 24 vCPU and writeback from 1 GiB.
export default line({
  alt: "Cores used by the three brokers together at 3,000,000 msg/s against the partition count (log scale). Queen uses 18.6 cores at 10 partitions and 22.4 to 23.4 from 100 to 10,000,000. Kafka uses 14.9 at 10, 18.5 at 100, 21.0 at 1,000, 52.8 at 10,000 and 57.0 at 25,000, then 50.2 at 50,000 and 60.5 at 100,000, where it did not carry the rate.",
  caption: "Queen's CPU does not move with the partition count. Kafka uses a little less up to 1,000 partitions, more than twice Queen's at 10,000, 57 cores at 25,000, and fails at 50,000 and 100,000 (hollow).",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs/matrix-gcp24-3m.json (Queen); report/report.py over the *-3000k-gcp24-xfs-t24 Kafka runs",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "cores, three brokers together (of 72)", ticks: [0, 20, 40, 60], min: 0, max: 64 },
  series: [
    { label: "Queen 2.3.0", tone: "s1", points: [[10, 18.6], [100, 23.0], [1000, 22.4], [10000, 23.3], [100000, 23.4], [1000000, 23.0], [10000000, 23.4]] },
    { label: "Kafka 4.3.1", points: [[10, 14.9], [100, 18.5], [1000, 21.0], [10000, 52.8], [25000, 57.0], { x: 50000, y: 50.2, hollow: true }, { x: 100000, y: 60.5, hollow: true }] },
  ],
});
