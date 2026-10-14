import { line } from "@/lib/figure-spec";

// The README's chart (assets/bench-queen-e2e-{light,dark}.svg), captured from the docs renderer on 2026-10-14; not on a
// docs page. Queen 2.3.0 on three GCP c4-standard-24-lssd brokers: e2e p99, the slowest load process's worst 10 s
// window in seconds 30 to 60, at 3M msg/s of 256-byte messages (runs m3-*) and 1M msg/s of 1 KB messages (runs p1k-*).
export default line({
  alt: "Queen 2.3.0 end-to-end p99 against the partition count from 10 to 10,000,000 on three GCP nodes with local NVMe: at 3,000,000 msg/s of 256-byte messages 78, 65, 56, 66, 71, 47 and 54 ms; at 1,000,000 msg/s of 1 KB messages 68, 34, 27, 28, 27, 29 and 65 ms.",
  caption: "Queen 2.3.0, e2e p99 by partition count at 3M msg/s of 256-byte messages and 1M msg/s of 1 KB messages.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs, the m3-* and p1k-* folders",
  x: { label: "partitions in one queue", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 20, 40, 60, 80], min: 0, max: 90 },
  width: 820,
  height: 330,
  series: [
    { label: "3M msg/s, 256 B", tone: "s1", points: [[10, 78], [100, 65], [1000, 56], [10000, 66], [100000, 71], [1000000, 47], [10000000, 54]] },
    { label: "1M msg/s, 1 KB", points: [[10, 68], [100, 34], [1000, 27], [10000, 28], [100000, 27], [1000000, 29], [10000000, 65]] },
  ],
});
