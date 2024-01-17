# Wildfire
## Why another fork from Apache Spark?
Over past 9 years since I dived into apache spark internals, as part of working for SnappyData, WorkDay and presently Cloudera, have come across unsual cases where Query performance have been far from satisfactorily.
These types of queries have extremely large complex expressions ( like involving case when etc) or very large number of nodes ( like Projects, Filters etc) in a Query Plan . These nodes could run into millions!. Such types of queries usually get generated via some user code, executing in a loop.

Apache Spark is a brilliantly written product, usually impressive in performance and rich in functionality. In some cases, though, as mentioned above, the engine is not able to cope up.

Upfront, I want to say, if your queries are reasonable in size, and the compilation happens within milliseconds or seconds and your runtime performance is satisfactory, then stick with Stock Spark.

But if query compilation times are running into hours or have large nested BroadcastHashJoins on columns which are not **partitioning columns** , may be this fork will be able to solve that issue. In no situation, should query compilation time, for humongous queries, exceed few minutes.

Based on my experience with debugging and fixing the performance issues ( compile time and runtime), following are the areas where usually the bottleneck shows up.

Coming to compile time bottlenecks:
### Compile time bottlenecks

1) Running each rule ( of analyzer, and optimizer) sequentially, would result in tree traversal that many times. It is possible to combine some of the rules and execute them together in a single pass, there by minimizing tree traversal.
2) There are places where the code is recursive. Flattening the recursion helps a lot.
3) Queries with large number of aliases and having nested/complex case when expressions,  can cause ConstraintPropagation rule to blow up. Stock spark's ConstraintPropagation rule creates constraints pessimistically and is combinatorial in nature. This can result in compilation times getting increased by magnitude.
4) Using DataFrame APIs to generate a query tree by adding on projects/filters can cause a huge tree, and every subsequent addition of filter/project would result in a repeat analysis of the subtree.
5) The Optimizer rules run in batches. A Batch would continue to run , till either of the two conditions is met. a) Plan becomes idempotent or 2) Max times to run limit is reached.  Now in a given batch, it is possible that only one rule is changing the plan, but it would cause other rules to still traverse the tree, without them affecting the plan.
6) The Predicate Pushdown does not push all the filters in a single pass and with each push the filter is re-aliased. This results in plan not reaching idempotency early and because filters are re-aliased from top - to bottom, instead of bottom to Top, the re-aliasing becomes inefficient as the filter keeps getting pushed down. Currently the tree to be substituted is large, while the substitue is small. This impacts the perf as tree to traverse is large. But if the tree to traverse is small, but the to be substituted value is large, the traversal cost is reduced.

### Runtime Perf improvment
The existing concept of Dynamic Partition Pruning( DPP ) can be extended to Broadcast Hash Joins involving **non partitioning *comparable* columns **, too. Spark engine already broadcasts the join keys before executing the Broadcast Hash Join. So the keys can be used to filter the rows of the streaming side, at the Scan Level, thereby using it to skip Data Blocks where ever a Min/Max stats is available.

This requires support from the DataSource Implementation too. I am calling it BroadcastVarPushdown

As of now, I have created the support for BroadcastVarPushdown for Iceberg using parquet format. 

If you want to investigate the perf behaviour on any other V2 DataSource which works along with Spark and code base is available, please reach out to me. I will be more than happy to figure out a workable solution.


As of now all the changes in this repo have a corresponding PR opened with upstream Spark, with test coverage. Its difficult to get it upmerged because of multiple reasons, like a) Not being a Committer b) Changes are extensive which rightly raise possibility of destabilizing c) The perf issues affect a niche class of queries and hence risk/reward is not justifiable etc.

If you have any questions, suggestions please do let me know.

For your reference I have put a small test in this Repo, which highlights the compile time issue due to ConstraintsProp rule. I will be adding other comparable tests too
**org.apache.spark.sql.catalyst.plans.CompareNewAndOldConstraintsSuite**
You can run this test on this master vs as well as copy and run in stock spark master.

