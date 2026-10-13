import { line } from "@/lib/figure-spec";

// benchmarks/partitions.mdx, "Every load at every count": the p99 halves of the
// table, Queen 2.3.0 pre-release builds, 2026-10-12 and 13. One line per load,
// c1 to c5 so each load has its own colour.
export default line({
  alt: "Queen 2.3.0: p99 end-to-end latency against the partition count of one queue, from 10 to 10,000,000 on a log scale, one line per load. At 100,000 msg/s it stays between 6 and 8 ms. At 500,000 between 9 and 12 ms. At 1,000,000 between 18 and 21 ms up to 100,000 partitions, 32 ms at a million and 38 ms at ten million. At 1,500,000 between 36 and 41 ms from 100 partitions up, and 57 ms at 10. At 2,000,000 between 59 and 73 ms from 100 partitions up, and 100 ms at 10.",
  caption: "Each load is a flat line from 100 to ten million partitions. The left end rises at the two highest loads because ten partitions are read by nine consumers; the rise of the 1,000,000 line at the right is one load process's one window (the median process reads 23 and 20 ms there).",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs/grid-final.jsonl",
  x: { label: "partitions in the queue", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 20, 40, 60, 80, 100], min: 0, max: 110 },
  series: [
    { label: "100,000 msg/s", tone: "c1", points: [[10, 8], [100, 6], [1000, 8], [10000, 7], [100000, 7], [1000000, 7], [10000000, 7]] },
    { label: "500,000", tone: "c2", points: [[10, 11], [100, 9], [1000, 10], [10000, 10], [100000, 10], [1000000, 12], [10000000, 10]] },
    { label: "1,000,000", tone: "c3", points: [[10, 21], [100, 19], [1000, 18], [10000, 19], [100000, 18], [1000000, 32], [10000000, 38]] },
    { label: "1,500,000", tone: "c4", points: [[10, 57], [100, 41], [1000, 36], [10000, 37], [100000, 38], [1000000, 36], [10000000, 37]] },
    { label: "2,000,000", tone: "c5", points: [[10, 100], [100, 63], [1000, 70], [10000, 63], [100000, 59], [1000000, 66], [10000000, 73]] },
  ],
});
