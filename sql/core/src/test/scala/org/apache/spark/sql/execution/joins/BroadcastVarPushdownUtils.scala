/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.joins

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, PropagateEmptyRelation, PruneFilters, PushDownPredicates}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{BufferedRows, InMemoryTable}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.execution.{SparkPlan, WrapsBroadcastVarPushDownSupporter}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper trait that enables BroadcastFilterPushdown for all tests regardless of default config
 */
trait EnableBroadcastFilterPushdownSuite extends SQLTestUtils {
  abstract override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN.key -> "true") {
        testFun
      }
    }
  }
}

/**
 * Helper trait that disables BroadcastFilterPushdown for all tests regardless of default config
 * values.
 */
trait DisableBroadcastFilterPushdownSuite extends SQLTestUtils {
  abstract override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN.key -> "false") {
        testFun
      }
    }
  }
}

trait BroadcastVarPushdownUtils extends SQLTestUtils {

  import scala.jdk.CollectionConverters._

  val non_part_table1_name = "non_part_tab1"
  val non_part_table2_name = "non_part_tab2"
  val non_part_table3_name = "non_part_tab3"

  val non_part_table1_schema =
    StructType.fromDDL("c1_1 INT, c1_2 INT, c1_3 LONG, c1_4 STRING, c1_5 STRING")
  val non_part_table2_schema =
    StructType.fromDDL("c2_1 INT, c2_2 INT, c2_3 LONG, c2_4 STRING, c2_5 STRING")
  val non_part_table3_schema =
    StructType.fromDDL("c3_1 INT, c3_2 INT, c3_3 LONG, c3_4 STRING, c3_5 STRING")

  val part_table1_schema =
    StructType.fromDDL("c1_1 INT, c1_2 INT, c1_3 LONG, c1_4 STRING, c1_5 STRING")
  val part_table2_schema =
    StructType.fromDDL("c2_1 INT, c2_2 INT, c2_3 LONG, c2_4 STRING, c2_5 STRING")
  val part_table3_schema =
    StructType.fromDDL("c3_1 INT, c3_2 INT, c3_3 LONG, c3_4 STRING, c3_5 STRING")

  def createNonPartitionedRelation(
      tablename: String,
      schema: StructType,
      data: BufferedRows): LogicalPlan

  def createPartitionedRelation(
      tablename: String,
      schema: StructType,
      datas: IndexedSeq[BufferedRows],
      partitionColName: String): LogicalPlan

  def non_part_table1: LogicalPlan = {
    val data = new BufferedRows()
    for (i <- 1 to 500) {
      data.withRow(
        new GenericInternalRow(
          Array(
            i,
            i * 2,
            (i * 2).toLong,
            UTF8String.fromString(s"name$i"),
            UTF8String.fromString(s"address$i"))))
    }
    createNonPartitionedRelation(non_part_table1_name, non_part_table1_schema, data)
  }

  def non_part_table2: LogicalPlan = {
    val data = new BufferedRows()
    for (i <- 1 to 10000) {
      data.withRow(
        new GenericInternalRow(
          Array(
            i,
            i * 2,
            (i * 4).toLong,
            UTF8String.fromString(s"name${i * 10}"),
            UTF8String.fromString(s"address${i * 10}"))))
    }

    createNonPartitionedRelation(non_part_table2_name, non_part_table2_schema, data)
  }

  def non_part_table3: LogicalPlan = {
    val data = new BufferedRows()
    for (i <- 1 to 800) {
      data.withRow(
        new GenericInternalRow(
          Array(
            i,
            i * 4,
            (i * 1).toLong,
            UTF8String.fromString(s"name${i * 20}"),
            UTF8String.fromString(s"address${i * 20}"))))
    }
    createNonPartitionedRelation(non_part_table3_name, non_part_table3_schema, data)
  }

  val part_table1_name = "part_tab1"
  val part_table2_name = "part_tab2"
  val part_table3_name = "part_tab3"

  def part_table1: LogicalPlan = {
    val datas = for (j <- 1 to 10) yield {
      val data = new BufferedRows(Seq(j))
      for (i <- 1 to 10) {
        data.withRow(
          new GenericInternalRow(
            Array(
              j,
              i * 10,
              (i * 20).toLong,
              UTF8String.fromString(s"name$i"),
              UTF8String.fromString(s"address$i"))))
      }
      data
    }
    createPartitionedRelation(part_table1_name, part_table1_schema, datas, "c1_1")
  }

  def part_table2: LogicalPlan = {
    val datas = for (j <- 1 to 10) yield {
      val data = new BufferedRows(Seq(j))
      for (i <- 1 to 100) {
        data.withRow(
          new GenericInternalRow(
            Array(
              j,
              i * 5,
              (i * 15).toLong,
              UTF8String.fromString(s"name${i * 10}"),
              UTF8String.fromString(s"address${i * 10}"))))
      }
      data
    }

    createPartitionedRelation(part_table2_name, part_table2_schema, datas, "c2_1")

  }

  def part_table3: LogicalPlan = {
    val datas = for (j <- 1 to 10) yield {
      val data = new BufferedRows(Seq(j))
      for (i <- 1 to 300) {
        data.withRow(
          new GenericInternalRow(
            Array(
              j,
              i * 20,
              (i * 25).toLong,
              UTF8String.fromString(s"name${i * 20}"),
              UTF8String.fromString(s"address${i * 20}"))))
      }
      data
    }

    createPartitionedRelation(part_table3_name, part_table3_schema, datas, "c3_1")
  }

  def createNonPartTable(
      name: String,
      schema: StructType,
      data: BufferedRows): LogicalPlan = DataSourceV2Relation.create(
    new InMemoryTable(name, schema, Array.empty, Map.empty[String, String].asJava)
      .withData(Array(data)),
    None,
    None)

  val emptyBatchScanToBuildLegMap =
    new java.util.IdentityHashMap[WrapsBroadcastVarPushDownSupporter, Any]()
  val emptyBuildLegsBlockingPushFromAncestors = new java.util.IdentityHashMap[SparkPlan, Any]()

  def assertPushdownData(sparkPlan: SparkPlan, expected: Seq[BroadcastVarPushDownData]): Unit = {
    val conf = sparkPlan.conf
    val bhjs = sparkPlan.collect { case bhj: BroadcastHashJoinExec =>
      bhj
    }
    val bhjsData = bhjs.map(getComponents)
    val result = bhjsData.flatMap(bhjData =>
      BroadcastHashJoinUtil.canPushBroadcastedKeysAsFilter(
        conf,
        bhjData.streamKeys,
        bhjData.buildKeys,
        bhjData.streamPlan,
        bhjData.buildPlan,
        emptyBatchScanToBuildLegMap))
    assertResult(expected.size)(result.size)
    val mutableExpected = scala.collection.mutable.ListBuffer.apply(expected: _*)
    result.foreach(x => {
      assert(mutableExpected.contains(x))
      mutableExpected -= x
    })
  }

  def getComponents(bhj: BroadcastHashJoinExec): BHJData = bhj.buildSide match {
    case BuildRight => BHJData(bhj.right, bhj.left, bhj.rightKeys, bhj.leftKeys)
    case BuildLeft => BHJData(bhj.left, bhj.right, bhj.leftKeys, bhj.rightKeys)
  }

  def getBatchScans(plan: SparkPlan, schema: StructType): Seq[WrapsBroadcastVarPushDownSupporter] =
    plan
    .collectLeaves()
    .filter(leaf => leaf match {
      case wb: WrapsBroadcastVarPushDownSupporter =>
        wb.getBroadcastVarPushDownSupportingInstance.exists(_.readSchema == schema)

      case _ => false

    }).map(_.asInstanceOf[WrapsBroadcastVarPushDownSupporter])

  protected def runWithDefaultConfig[T](func: => T): T = {
    withSQLConf(
      SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN.key -> "true",
      SQLConf.PREFER_AS_BUILDSIDE_LEG_ALREADY_BROADCASTED.key -> "true",
      SQLConf.PREFER_BROADCAST_VAR_PUSHDOWN_OVER_DPP.key -> "true") {
      func
    }
  }

  protected def runWithPushPredRuleOff[T](func: => T): T = {
    withSQLConf(
      SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN.key -> "true",
      SQLConf.PREFER_AS_BUILDSIDE_LEG_ALREADY_BROADCASTED.key -> "true",
      (SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
        s"${PushDownPredicates.ruleName},${PropagateEmptyRelation.ruleName}," +
          s"${PruneFilters.ruleName}"),
      SQLConf.PREFER_BROADCAST_VAR_PUSHDOWN_OVER_DPP.key -> "true") {
      func
    }
  }

  protected def runWithBroadcastVarPushOff[T](func: => T): T = {
    withSQLConf(
      SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN.key -> "false",
      SQLConf.PREFER_AS_BUILDSIDE_LEG_ALREADY_BROADCASTED.key -> "true",
      SQLConf.PREFER_BROADCAST_VAR_PUSHDOWN_OVER_DPP.key -> "false") {
      func
    }
  }

  protected def runWithIntactDPP[T](func: => T): T = {
    withSQLConf(
      SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN.key -> "true",
      SQLConf.PREFER_AS_BUILDSIDE_LEG_ALREADY_BROADCASTED.key -> "true",
      SQLConf.PREFER_BROADCAST_VAR_PUSHDOWN_OVER_DPP.key -> "false") {
      func
    }
  }
}

case class BHJData(
    buildPlan: SparkPlan,
    streamPlan: SparkPlan,
    buildKeys: Seq[Expression],
    streamKeys: Seq[Expression])

trait DataSourceV2RelationCreator {
  this : BroadcastVarPushdownUtils =>

  import scala.jdk.CollectionConverters._

  override def createNonPartitionedRelation(
      tablename: String,
      schema: StructType,
      data: BufferedRows): LogicalPlan = {
    DataSourceV2Relation.create(
      new InMemoryTable(
        tablename,
        schema,
        Array.empty,
        Map.empty[String, String].asJava).withData(Array(data)),
      None,
      None)
  }

  override def createPartitionedRelation(
      tablename: String,
      schema: StructType,
      datas: IndexedSeq[BufferedRows],
      partitionColName: String): LogicalPlan = {
    DataSourceV2Relation.create(
      new InMemoryTable(
        tablename,
        schema,
        Array(IdentityTransform(FieldReference(partitionColName))),
        Map.empty[String, String].asJava).withData(datas.toArray),
      None,
      None)
  }
}
