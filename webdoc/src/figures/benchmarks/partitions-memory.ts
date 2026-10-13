import { line } from "@/lib/figure-spec";

// benchmarks/partitions.mdx, "CPU and memory": anonymous memory at 100,000
// msg/s, seconds 30 to 60, Queen 2.3.0 pre-release builds, 2026-10-12 and 13.
// The ten-million point ran with QUEEN_CONSUME_IDLE_UNLOAD_S=30.
export default line({
  alt: "Queen 2.3.0 at 100,000 msg/s: anonymous memory against the number of partitions in the queue. The leader holds 0.5 GB with 1,000 partitions, 0.9 GB with 100,000, 2.5 GB with 1,000,000 and 11.4 GB with 10,000,000. A follower holds 0.4, 0.5, 1.4 and 9.8 GB.",
  caption: "A straight line: memory is what a partition costs, about 1 KB on every node and a little more on the leader, so memory sets the partition ceiling.",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs/grid-final.jsonl",
  x: { label: "partitions in the queue", ticks: [0, 2500000, 5000000, 7500000, 10000000], min: 0, max: 10500000 },
  y: { label: "memory (anonymous RSS)", ticks: [0, 3, 6, 9, 12], min: 0, max: 13, format: "gb" },
  height: 260,
  series: [
    { label: "Leader", points: [[1000, 0.5], [100000, 0.9], [1000000, 2.5], [10000000, 11.4]] },
    { label: "Follower", points: [[1000, 0.4], [100000, 0.5], [1000000, 1.4], [10000000, 9.8]] },
  ],
});
