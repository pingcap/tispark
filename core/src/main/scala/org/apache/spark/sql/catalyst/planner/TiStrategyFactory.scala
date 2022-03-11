/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.planner

import com.pingcap.tispark.utils.ReflectionUtil
import org.apache.spark.sql.{SparkSession, Strategy, TiContext, TiExtensions}

/**
 * The logical plan DataSourceV2ScanRelation is different in spark 3.0 and 3.1
 * @param getOrCreateTiContext
 */
class TiStrategyFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Strategy) {
  override def apply(sparkSession: SparkSession): Strategy = {
    TiExtensions.validateCatalog(sparkSession)
    ReflectionUtil.newTiStrategy(getOrCreateTiContext, sparkSession)
  }
}
