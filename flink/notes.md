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

Sample repo: https://github.com/largecats/streaming-notes/tree/main/flink/word_count

**Adding dependencies in sbt.** Need to add like this: 

```
libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % "1.12.2" % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % "1.12.2" % "provided",
  "org.apache.flink" %% "flink-examples-streaming" % "1.12.2" % "provided",
  ...
  )
```

The `% provided` is required to scope the dependencies. Otherwise might throw this error:

![](images/sbt_scope_error.png)

Can search in [Maven repository](https://mvnrepository.com/search?q=flink+scala) for the dependency names.

**Submitting job to cluster.** Submit via 

```
/mnt/c/flink-1.12.2/bin/flink run target/scala-2.11/wordCount.jar
```

#### TBD

**Observation.** Only statements printed via `println("xxx")`:

![Console output of the WordCount example.](images/console.png)

The output of `counts.print()` is not printed to console but stored in `/mnt/c/flink-1.12.2/log/flink-*-taskexecutor-*.out`:

![Output of counts.print() in log file.](images/log.png)

And can be viewed in Flink UI's task manager stdout:

![](images/task_manager_stdout.png)

**Investigations.** The difference is because `println("xxx")` prints to console by definition:

![](images/println_definition.png)

And `counts.print()` prints to stdout by definition: 

![](images/datastream_print_definition.png)

**Questions.**

1. Is there a way to print the result of `counts.print()` to console when running a local Flink cluster? 

   1. https://stackoverflow.com/questions/58209372/flink-dont-print-the-object-in-the-std-output

2. Is there a way to do the same when running a Fink cluster on YARN? E.g., have the flink application run on client (like client mode in Spark)?

   1. From https://ci.apache.org/projects/flink/flink-docs-stable/deployment/#deployment-modes, seems that "application mode" corresponds to cluster mode, and "per-job mode" corresponds to client mode.

   2. To verify, tried starting flink on yarn: https://ci.apache.org/projects/flink/flink-docs-stable/deployment/resource-providers/yarn.html#starting-a-flink-session-on-yarn, but encountered following error. Probably due to version issue.

      ![](images/flink_on_yarn_error.png)

3. Will we be able to use YARN's log aggregation after integrating Flink with YARN?

