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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.SupportsBroadcastVarPushdownFiltering
import org.apache.spark.sql.execution.joins.ProxyBroadcastVarAndStageIdentifier
import org.apache.spark.sql.types.StructType

trait WrapsBroadcastVarPushDownSupporter {
  self: LeafExecNode =>
  @transient val proxyForPushedBroadcastVar: Option[Seq[ProxyBroadcastVarAndStageIdentifier]]

  def getBroadcastVarPushDownSupportingInstance: Option[SupportsBroadcastVarPushdownFiltering]

  def resetFilteredPartitionsAndInputRdd(): Unit

  def newInstance(proxy: Option[Seq[ProxyBroadcastVarAndStageIdentifier]],
      runtimeFilters: Seq[Expression]): WrapsBroadcastVarPushDownSupporter

  def newInstance(proxy: Option[Seq[ProxyBroadcastVarAndStageIdentifier]]):
  WrapsBroadcastVarPushDownSupporter

  def getTableIdentifier(): Option[TableIdentifier]

  def getSchema(): StructType

  def setLogicalLink(logicalPlan: LogicalPlan): Unit

  def output: Seq[Attribute]

  def containsNonBroadcastVarRuntimeFilters: Boolean

  def getNonBroadcastVarRuntimeFilters: Seq[Expression]
}
