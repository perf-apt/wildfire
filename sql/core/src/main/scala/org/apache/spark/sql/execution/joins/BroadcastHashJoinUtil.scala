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

import scala.collection.mutable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal}
// import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.SupportsBroadcastVarPushdownFiltering
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.dynamicpruning.PartitionPruning
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, ObjectType}


object BroadcastHashJoinUtil {
  def canPushBroadcastedKeysAsFilter(
      conf: SQLConf,
      streamJoinKeys: Seq[Expression],
      buildJoinKeys: Seq[Expression],
      streamPlan: SparkPlan,
      buildPlan: SparkPlan,
      batchScansSelectedForBCPush: java.util.IdentityHashMap[WrapsBroadcastVarPushDownSupporter, _])
      : Seq[BroadcastVarPushDownData] = {
    if (conf.pushBroadcastedJoinKeysASFilterToScan &&
      !BroadcastHashJoinUtil.getAllBatchScansForSparkPlan(buildPlan.canonicalized).sameElements(
        BroadcastHashJoinUtil.getAllBatchScansForSparkPlan(streamPlan.canonicalized)) &&
      isBuildPlanPrunable(
        buildPlan,
        batchScansSelectedForBCPush)) {
      getPushDownDataSkipBuildSideCheck(
        conf,
        streamJoinKeys,
        buildJoinKeys,
        streamPlan,
        buildPlan,
        batchScansSelectedForBCPush)
    } else {
      Seq.empty
    }
  }

  def pushBroadcastVar(
      bcRelation: Broadcast[HashedRelation],
      buildKeysCanonicalized: Seq[Expression],
      pushDownData: Seq[BroadcastVarPushDownData]): Unit = {
      pushDownData.groupBy(pdd => pdd.targetBatchScanExec).values.foreach(
        pushBroadcastVarForBatchScan(bcRelation, buildKeysCanonicalized, _))
  }

  def pushBroadcastVarForBatchScan(
      bcRelation: Broadcast[HashedRelation],
      buildKeysCanonicalized: Seq[Expression],
      pushDownData: Seq[BroadcastVarPushDownData]): Unit = {
    val totalJoinKeys = buildKeysCanonicalized.size
    val dataTypesArray = buildKeysCanonicalized.map(_.dataType).toArray
    pushDownData.foreach { bcData =>
      val streamJoinLeafColName = getColNameFromUnderlyingScan(
        bcData.targetBatchScanExec.getBroadcastVarPushDownSupportingInstance.get,
        bcData.streamsideLeafJoinAttribIndex)
      val actualData = new BroadcastedJoinKeysWrapperImpl(
        bcRelation,
        dataTypesArray,
        bcData.joinKeyIndexInJoiningKeys,
        totalJoinKeys)
      val dt = ObjectType(classOf[BroadcastedJoinKeysWrapperImpl])
      val embedAsLiteral = Literal.create(actualData, dt)
      val filter = org.apache.spark.sql.sources.In(streamJoinLeafColName, Array(embedAsLiteral))
      bcData.targetBatchScanExec.getBroadcastVarPushDownSupportingInstance.get
        .filter(Array(filter.toV2))
    }
    pushDownData.headOption.foreach(_.targetBatchScanExec.resetFilteredPartitionsAndInputRdd())
  }

  def getColNameFromUnderlyingScan(scan: SupportsBroadcastVarPushdownFiltering, index: Int):
  String = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    scan.allAttributes()(index).fieldNames().toSeq.quoted
  }

  def partitionBatchScansToReadyAndUnready(
      stage: QueryStageExec,
      cachedBatchScans: mutable.Map[Int, Seq[WrapsBroadcastVarPushDownSupporter]])
      : (Seq[WrapsBroadcastVarPushDownSupporter], Seq[WrapsBroadcastVarPushDownSupporter]) =
    cachedBatchScans.getOrElseUpdate(stage.id, getAllBatchScansForStage(stage))
    .partition(isBatchScanReady)

  // Not to be invoked for Stage. this is used only for join condition
  // identification. it goes below a query stage it it encounters one
  def getAllBatchScansForSparkPlan(
      plan: SparkPlan,
      goInsideStageExec: Boolean = true): Seq[WrapsBroadcastVarPushDownSupporter] =
    plan.collectLeaves().flatMap {
      case bs: WrapsBroadcastVarPushDownSupporter => Seq(bs)
      case qs: QueryStageExec if goInsideStageExec => getAllBatchScansForSparkPlan(qs.plan)
      case re: ReusedExchangeExec if goInsideStageExec => getAllBatchScansForSparkPlan(re.child)
      case _ => Seq.empty
    }

  def isStageReadyForMaterialization(
      stage: QueryStageExec,
      cachedBatchScans: mutable.Map[Int, Seq[WrapsBroadcastVarPushDownSupporter]]): Boolean =
    cachedBatchScans
      .getOrElseUpdate(stage.id, getAllBatchScansForStage(stage))
      .forall(isBatchScanReady)

  def getPushdownDataForBatchScansUsingJoinKeys(
      canonicalizedBuildKeys: Seq[Expression],
      streamPlan: SparkPlan,
      buildPlanAndProxies: (LogicalPlan, Seq[ProxyBroadcastVarAndStageIdentifier]))
      : Seq[BroadcastVarPushDownData] = {
    val (buildLp, buildLegProxies) = buildPlanAndProxies
    streamPlan
      .collectLeaves()
      .flatMap {
        case bs: WrapsBroadcastVarPushDownSupporter => Seq(bs)

        case _ => Seq.empty
      }
      .filter(bs => bs.proxyForPushedBroadcastVar.isDefined && !isBatchScanReady(bs))
      .flatMap(bs => {
        val jkdsOfInterest = bs.proxyForPushedBroadcastVar.get.collect {
          case proxy
              if (proxy.buildLegPlan.eq(buildLp) || proxy.buildLegPlan.canonicalized ==
                buildLp.canonicalized) && (
                proxy.buildLegProxyBroadcastVarAndStageIdentifiers.isEmpty || (
                  buildLegProxies.size == proxy.buildLegProxyBroadcastVarAndStageIdentifiers.size &&
                  buildLegProxies.forall(
                  proxy.buildLegProxyBroadcastVarAndStageIdentifiers.contains))) =>
            proxy.joiningKeysData.filter(jkd =>
              canonicalizedBuildKeys.contains(jkd.buildSideJoinKeyAtJoin))
        }.flatten
        jkdsOfInterest.map(jkd =>
          BroadcastVarPushDownData(
            jkd.streamsideLeafJoinAttribIndex,
            bs,
            jkd.joiningColDataType,
            jkd.joinKeyIndexInJoiningKeys))
      })
  }

  def isBatchScanReady(batchScanExec: WrapsBroadcastVarPushDownSupporter): Boolean =
    batchScanExec.getBroadcastVarPushDownSupportingInstance match {
    case Some(sr) =>
      val totalBCVars = batchScanExec.proxyForPushedBroadcastVar.fold(0)(_.foldLeft(0) {
        case (num, proxy) => num + proxy.joiningKeysData.size
      })
      totalBCVars <= sr.getPushedBroadcastFiltersCount()

    case _ => true
  }

  def convertJoinKeyDataToPushDownData(
      bs: WrapsBroadcastVarPushDownSupporter,
      jkd: JoiningKeyData): BroadcastVarPushDownData = BroadcastVarPushDownData(
    jkd.streamsideLeafJoinAttribIndex,
    bs,
    jkd.joiningColDataType,
    jkd.joinKeyIndexInJoiningKeys)

  def convertNameReferencesToString(ref: NamedReference): String = {
    val seq = ref.fieldNames.toSeq
    seq.map(quoteIfNeeded).mkString(".")
  }

  /*
  def getLogicalPlanFor(plan: SparkPlan): LogicalPlan = {
    val logicalNodeOpt = plan.logicalLink.orElse(plan.collectFirst {
      case p if p.logicalLink.isDefined => p.logicalLink.get
    })
    assert(logicalNodeOpt.isDefined)
    def deconstructLogicalQueryStage(logicalPlan: LogicalPlan): LogicalPlan = {
      logicalPlan transformUp {
        case LogicalQueryStage(lp, _) => deconstructLogicalQueryStage(lp)
      }
    }

    logicalNodeOpt.map(deconstructLogicalQueryStage).get
  }
   */
  def getOriginalLogicalPlanForBuildPlan(joinPlan: LogicalPlan): LogicalPlan =
    joinPlan.getTagValue(Join.PRESERVE_JOIN_WITH_SELF_PUSH_HASH).map(_._2).get

  private def getPushDownDataSkipBuildSideCheck(
      conf: SQLConf,
      streamJoinKeys: Seq[Expression],
      buildJoinKeys: Seq[Expression],
      streamPlan: SparkPlan,
      buildPlan: SparkPlan,
      batchScansSelectedForBCPush: java.util.IdentityHashMap[WrapsBroadcastVarPushDownSupporter, _])
      : Seq[BroadcastVarPushDownData] = {
    val streamKeysStart = streamJoinKeys.zipWithIndex.filter { case (streamJk, _) =>
      streamJk.isInstanceOf[Attribute]
    }
    (for ((streamKeyStartExp, joinKeyIndex) <- streamKeysStart) yield {
      val streamKeyStart = streamKeyStartExp.asInstanceOf[Attribute]
      val batchScansOfInterest = identifyBatchScanOfInterest(
        streamKeyStart,
        streamPlan,
        batchScansSelectedForBCPush)
      val filteredBatchScansOfInterest =
        batchScansOfInterest.flatMap { case (currentStreamKey, runtimeFilteringBatchScan) =>
          val underlyingRuntimeFilteringScan =
            runtimeFilteringBatchScan.getBroadcastVarPushDownSupportingInstance.get
          val streamKey = currentStreamKey
          val streamsideLeafJoinAttribIndex = runtimeFilteringBatchScan.output.indexWhere(
            _.canonicalized == streamKey.canonicalized)
          if (underlyingRuntimeFilteringScan.allAttributes().nonEmpty) {
            val streamsideJoinColName = getColNameFromUnderlyingScan(
              underlyingRuntimeFilteringScan,
              streamsideLeafJoinAttribIndex)
            val partitionCols = underlyingRuntimeFilteringScan.partitionAttributes().
              map(convertNameReferencesToString).toSet
            // TODO Asif: Fix completely the push downn of broadcast var for partitioning column
            // then the below condition should be removed.
            if (!partitionCols.contains(streamsideJoinColName)) {
              if (!runtimeFilteringBatchScan.containsNonBroadcastVarRuntimeFilters) {
                Seq(
                  BroadcastVarPushDownData(
                    streamsideLeafJoinAttribIndex,
                    runtimeFilteringBatchScan,
                    buildJoinKeys(joinKeyIndex).dataType,
                    joinKeyIndex))
              } else if (conf.preferBroadcastVarPushdownOverDPP) {
                // TODO: Asif :because of bug in spark where if a union node contains two tables,
                // one partitioned and another non partitioned, spark assumes both are partitioned
                // and pushes run time filter (dynamic expression) on both.
                // so we need to tackle this here.

                // we are here means runtime filters added to batchscanexec is non empty
                val removeDpp = partitionCols.contains(streamsideJoinColName) ||
                  partitionCols.isEmpty
                Seq(
                  BroadcastVarPushDownData(
                    streamsideLeafJoinAttribIndex,
                    runtimeFilteringBatchScan,
                    buildJoinKeys(joinKeyIndex).dataType,
                    joinKeyIndex,
                    removeDpp))
              } else {
                Seq(
                  BroadcastVarPushDownData(
                    streamsideLeafJoinAttribIndex,
                    runtimeFilteringBatchScan,
                    buildJoinKeys(joinKeyIndex).dataType,
                    joinKeyIndex))
              }
            } else {
              Seq.empty
            }
          } else {
            Seq.empty
          }
        }
      filteredBatchScansOfInterest
    }).flatten
  }

  private def isBuildPlanPrunable(
      buildPlan: SparkPlan,
      batchScansSelectedForBCPush: java.util.IdentityHashMap[WrapsBroadcastVarPushDownSupporter, _])
  : Boolean = {
    val plansToCheck = mutable.ListBuffer[SparkPlan](buildPlan)
    val considerPushedBCVarAsPrunability =
      buildPlan.conf.considerPushedBroadcastvarOnBatchscanAsPrunablity
    var isBuildPlanPrunable = false
    while (plansToCheck.nonEmpty && !isBuildPlanPrunable) {
      val planToCheck = plansToCheck.remove(0)
      planToCheck match {
        case FilterExec(expr, _) if PartitionPruning.isLikelySelective(expr) =>
          isBuildPlanPrunable = true

        case bs: WrapsBroadcastVarPushDownSupporter
            if bs.proxyForPushedBroadcastVar.isDefined ||
              (batchScansSelectedForBCPush.ne(null) && batchScansSelectedForBCPush.containsKey(
                bs)) && considerPushedBCVarAsPrunability =>
          isBuildPlanPrunable = true

       // case _: BaseAggregateExec => isBuildPlanPrunable = true

      /*  case j: BaseJoinExec if j.joinType == LeftSemi || j.joinType == Inner =>
          isBuildPlanPrunable = true
      */
        case ree: ReusedExchangeExec => plansToCheck.prepend(ree.child)

        case x: QueryStageExec => plansToCheck.prepend(x.plan)

        case x: AdaptiveSparkPlanExec => plansToCheck.prepend(x.inputPlan)

        case rest => plansToCheck.prependAll(rest.children)
      }
    }
    isBuildPlanPrunable
  }

  private def identifyBatchScanOfInterest(
      streamKeyStart: Attribute,
      streamPlan: SparkPlan,
      batchScansSelectedForBCPush: java.util.IdentityHashMap[WrapsBroadcastVarPushDownSupporter, _])
      : Seq[(Attribute, WrapsBroadcastVarPushDownSupporter)] = {
    var currentStreamKey = streamKeyStart
    var currentStreamPlan = streamPlan
    var batchScanOfInterest = Seq.empty[(Attribute, WrapsBroadcastVarPushDownSupporter)]
    var keepGoing = true
    while (keepGoing) {
      currentStreamPlan match {

        case _: WindowExec => keepGoing = false

        case wb: WrapsBroadcastVarPushDownSupporter
          if wb.getBroadcastVarPushDownSupportingInstance.isDefined =>
          batchScanOfInterest = Seq(currentStreamKey -> wb)
          keepGoing = false

        case qse: QueryStageExec =>
          if (qse.isMaterialized) {
            keepGoing = false
          } else {
            currentStreamPlan = qse.plan
          }

        case _: ReusedExchangeExec => keepGoing = false
        /* currentStreamPlan = ree.child
          val indx = ree.output.indexWhere(_.canonicalized == currentStreamKey.canonicalized)
          currentStreamKey = ree.child.output(indx) */

        case _: AdaptiveSparkPlanExec => keepGoing = false

        case _: LeafExecNode => keepGoing = false

        /*
        case j: BaseJoinExec if !(j.joinType == LeftSemi || j.joinType == Inner) =>
                    keepGoing = false

         */

        case proj: ProjectExec =>
          proj.projectList.find(
            _.toAttribute.canonicalized ==
              currentStreamKey.canonicalized) match {
            case Some(_: Attribute) =>
            case Some(Alias(childExpr: Attribute, _)) => currentStreamKey = childExpr
            case _ => keepGoing = false
          }
          currentStreamPlan = proj.child

        case agg: BaseAggregateExec =>
          val ne = agg.resultExpressions
            .find(_.toAttribute.canonicalized == currentStreamKey.canonicalized)
            .get

          val groupNamedExprOpt = ne match {
            case attr: Attribute =>
              agg.groupingExpressions.find(_.toAttribute.canonicalized == attr.canonicalized)
            case Alias(attr: Attribute, _) =>
              agg.groupingExpressions.find(_.toAttribute.canonicalized == attr.canonicalized)
            case _ => None
          }
          groupNamedExprOpt match {
            case Some(attribute: Attribute) => currentStreamKey = attribute
            case Some(Alias(childExpr: Attribute, _)) => currentStreamKey = childExpr
            case _ => keepGoing = false
          }
          currentStreamPlan = agg.child

        case u: UnionExec =>
          val indexOfStreamCol =
            u.output.indexWhere(_.canonicalized == currentStreamKey.canonicalized)
          batchScanOfInterest = (for (child <- u.children) yield {
            identifyBatchScanOfInterest(
              child.output(indexOfStreamCol),
              child,
              batchScansSelectedForBCPush)
          }).flatten
          keepGoing = false

        case somePlan =>
          currentStreamPlan = somePlan.children
            .find(_.output.exists(_.canonicalized == currentStreamKey.canonicalized))
            .getOrElse({
              keepGoing = false
              null
            })
      }
    }
    batchScanOfInterest
  }

  // This function will not go below a QueryStageExec if it exists in a tree and
  // it does not have to as the batch scans below a query stage under a top stage are
  // already materialized. as new stage gets created only when child stage is materialized
  private def getAllBatchScansForStage(stage: QueryStageExec):
  Seq[WrapsBroadcastVarPushDownSupporter] =
    stage.plan.collectLeaves().collect { case bs: WrapsBroadcastVarPushDownSupporter =>
      bs
    }

  private def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }
}

case class BroadcastVarPushDownData(
    streamsideLeafJoinAttribIndex: Int,
    targetBatchScanExec: WrapsBroadcastVarPushDownSupporter,
    joiningColDataType: DataType,
    joinKeyIndexInJoiningKeys: Int,
    requiresDPPRemoval: Boolean = false)
