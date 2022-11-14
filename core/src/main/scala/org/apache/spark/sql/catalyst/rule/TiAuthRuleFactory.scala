/*
 * Copyright 2018 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.rule

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, TiContext, TiExtensions}
import org.slf4j.LoggerFactory

class TiAuthRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Rule[LogicalPlan]) {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    TiExtensions.validateCatalog(sparkSession)
    if (TiExtensions.authEnable(sparkSession)) {
      logger.info("TiSpark running in auth mode")
      TiAuthorization.enableAuth = true
      TiAuthorization.sqlConf = sparkSession.sqlContext.conf
      TiAuthorization.tiConf =
        TiUtil.sparkConfToTiConfWithoutPD(sparkSession.sparkContext.conf, new TiConfiguration())
    } else {
      TiAuthorization.enableAuth = false
    }
    TiAuthorizationRule(getOrCreateTiContext)(sparkSession)
  }
}