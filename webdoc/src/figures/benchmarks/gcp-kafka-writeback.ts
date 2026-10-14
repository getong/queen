import { bar } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "Kafka and the filesystem": Kafka 4.3.1's p99 at 3M msg/s, one setting changed at a
// time: ext4 with the harness's writeback (dirty_background_ratio = 5, about 3.6 GB of dirty pages on 88 GB); ext4 with
// dirty_background_bytes = 1 GiB; ext4 mounted `lazytime` with 1 GiB; XFS with 1 GiB and the thread pools sized for
// 24 vCPU (24 request handlers, 12 network threads, 12 replica fetchers). The median of the three windows in seconds
// 30 to 60; the worst windows are in the page's table.
export default bar({
  alt: "Kafka's p99 end-to-end latency at 3,000,000 msg/s by partition count, the median of three 10-second windows, with four settings: ext4 with writeback at 5% of memory; ext4 with writeback from 1 GiB; ext4 mounted lazytime with 1 GiB; XFS with 1 GiB and thread pools for 24 vCPU. 10 partitions: 938, 506, 272 and 11 ms. 100: 96, 50, 13 and 7.5. 1,000: 43, 88, 15 and 8.1. 10,000: 46, 61, 34 and 21.",
  caption: "Kafka's p99 at 3M msg/s, the median of three windows. Writeback from 1 GiB helped at 10 and 100 partitions only; ext4 lazytime cut the p99 at every count; XFS with thread pools for 24 vCPU brought it to 7.5 to 21 ms.",
  source: "benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the *-3000k-gcp24, *-3000k-gcp24-dbb1g, *-3000k-gcp24-lazy and *-3000k-gcp24-xfs-t24 folders",
  categories: ["10 partitions", "100", "1,000", "10,000"],
  x: { label: "p99 end-to-end latency, median window", format: "ms" },
  width: 880,
  series: [
    { label: "ext4, writeback at 5% of memory", tone: "s3", values: [938, 96, 43, 46] },
    { label: "ext4, writeback from 1 GiB", tone: "s2", values: [506, 50, 88, 61] },
    { label: "ext4 lazytime, 1 GiB", tone: "s4", values: [272, 13, 15, 34] },
    { label: "XFS, 1 GiB, threads for 24 vCPU", tone: "s1", values: [11, 7.5, 8.1, 21] },
  ],
});
