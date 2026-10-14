import { bar } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "Kafka and the page cache": Kafka 4.3.1's p99 at 3M msg/s with the harness's
// writeback (dirty_background_ratio = 5, about 3.6 GB of dirty pages on 88 GB) and with dirty_background_bytes = 1 GiB.
export default bar({
  alt: "Kafka's p99 end-to-end latency at 3,000,000 msg/s, by partition count, with background writeback starting at 5% of memory and at 1 GiB. 10 partitions: 1,401 ms and 514 ms. 100: 103 and 61. 1,000: 149 and 104. 10,000: 138 and 110.",
  caption: "Starting the background writeback at 1 GiB instead of 5% of memory cut Kafka's p99 by a factor of 1.25 to 2.7 on these brokers.",
  source: "benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the *-3000k-gcp24 and *-3000k-gcp24-dbb1g folders",
  categories: ["10 partitions", "100", "1,000", "10,000"],
  x: { label: "p99 end-to-end latency", format: "ms" },
  series: [
    { label: "writeback at 5% of memory", values: [1401, 103, 149, 138] },
    { label: "writeback at 1 GiB", tone: "s3", values: [514, 61, 104, 110] },
  ],
});
