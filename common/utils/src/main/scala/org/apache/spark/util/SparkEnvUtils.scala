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
package org.apache.spark.util

private[spark] trait SparkEnvUtils {

  /**
   * Indicates whether Spark is currently running unit tests.
   */
  def isTesting: Boolean = {
    // Scala's `sys.env` creates a ton of garbage by constructing Scala immutable maps, so
    // we directly use the Java APIs instead.
    (System.getenv("SPARK_TESTING") != null || System.getProperty("spark.testing") != null) &&
      System.getProperty("SPARK-45943") == null
  }

}

object SparkEnvUtils extends SparkEnvUtils
