# Wildfire
## Why another fork?
Over past 9 years since I dived into apache spark internals, as part of working for SnappyData, WorkDay and presently Cloudera, have come across unsual cases where Query performance have been far from satisfactorily.
These types of queries have extremely large complex expressions ( like involving case when etc) or very large number of nodes ( like Projects, Filters etc) in a Query Plan . These nodes could run into millions!. Such types of queries usually get generated via some user code, executing in a loop.
Apache Spark is a brilliantly written product, usually impressive in performance and rich in functionality. In some cases, though, as mentioned above, the engine is not able to cope up.
Based on my experience with debugging and fixing the performance issues ( compile time and runtime), following are the areas where usually the bottleneck shows up.
Upfront, I want to say, if your queries are reasonable in size, and the compilation happens within milliseconds or seconds, then your current code base is fine. In that case you may want to read section on runtime perf.
But if you queries compilation time are running into hours, may be this fork will be able to solve that issue. In no situation, should query compilation time, for humongous queries, exceed few minutes.

Coming to compile time bottlenecks:
### Compile time bottlenecks

1) Running each rule ( of analyzer, and optimizer) sequentially, would result in tree traversal that many times. It is possible to combine some of the rules and execute them together in a single pass, there by minimizing tree traversal.
2) There are places where the code is recursive. Flattening the recursion helps a lot.
3) Queries with large number of aliases and having nested/complex case when expressions,  can cause ConstraintPropagation rule to blow up. Stock spark's ConstraintPropagation rule creates constraints pessimistically and is combinatorial in nature. This can result in compilation times getting increased by magnitude.
4) Using DataFrame APIs to generate a query tree by adding on projects/filters can cause a huge tree, and every subsequent addition of filter/project would result in a repeat analysis of the subtree.
5) The Optimizer rules run in batches. A Batch would continue to run , till either of the two conditions is met. a) Plan becomes idempotent or 2) Max times to run limit is reached.  Now in a given batch, it is possible that only one rule is changing the plan, but it would cause other rules to still traverse the tree, without them affecting the plan.
6) The Predicate Pushdown does not push all the filters in a single pass and with each push the filter is re-aliased. This results in plan not reaching idempotency early and because filters are re-aliased from top - to bottom, instead of bottom to Top, the re-aliasing becomes inefficient as the filter keeps getting pushed down. Currently the tree to be substituted is large, while the substitue is small. This impacts the perf as tree to traverse is large. But if the tree to traverse is small, but the to be substituted value is large, the traversal cost is reduced.

### Runtime Perf improvment
1) 








# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing,
and Structured Streaming for stream processing.

<https://spark.apache.org/>

[![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_main.yml)
[![AppVeyor Build](https://img.shields.io/appveyor/ci/ApacheSoftwareFoundation/spark/master.svg?style=plastic&logo=appveyor)](https://ci.appveyor.com/project/ApacheSoftwareFoundation/spark)
[![PySpark Coverage](https://codecov.io/gh/apache/spark/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/spark)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/pyspark?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/pyspark/)


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](https://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](https://maven.apache.org/).
To build Spark and its example programs, run:

```bash
./build/mvn -DskipTests clean package
```

(You do not need to do this if you downloaded a pre-built package.)

More detailed documentation is available from the project site, at
["Building Spark"](https://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

```bash
./bin/spark-shell
```

Try the following command, which should return 1,000,000,000:

```scala
scala> spark.range(1000 * 1000 * 1000).count()
```

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

```bash
./bin/pyspark
```

And run the following command, which should also return 1,000,000,000:

```python
>>> spark.range(1000 * 1000 * 1000).count()
```

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

```bash
./bin/run-example SparkPi
```

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

```bash
MASTER=spark://host:7077 ./bin/run-example SparkPi
```

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

```bash
./dev/run-tests
```

Please see the guidance on how to
[run tests for a module, or individual tests](https://spark.apache.org/developer-tools.html#individual-tests).

There is also a Kubernetes integration test, see resource-managers/kubernetes/integration-tests/README.md

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version and Enabling YARN"](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
