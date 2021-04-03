# Notes

## Comparison with Structured Streaming

**Basic building block.** Structured streaming: batch. Flink: stream.

Structured streaming: Batch is batch. Stream is micro-batch (experimental: continuous processing).

Flink: Batch is bounded stream. Stream is unbounded stream.

**Data consistency guarantee.** End-to-end exactly-once fault-tolerance for both.

**APIs.** Structured streaming seems more SQL-like. Flink seems more MapReduce-like.

|                               | Structured Streaming                                         | Flink                                                        |
| ----------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Primitive operations          | MapReduce-like and SQL-like operations on DataFrame, Dataset, see [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#operations-on-streaming-dataframesdatasets) | MapReduce-like operations on DataStream, see [here](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/operators/) |
| Arbitrary stateful operations | `mapGroupsWithState`, `flatMapGroupsWithState`               | `ProcessFunction`                                            |



## Try Flink

### Local Installation

