/*
 * Copyright 2021 PingCAP, Inc.
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

package org.apache.spark.sql.catalyst.analyzer

import com.pingcap.tispark.MetaManager
import com.pingcap.tispark.auth.TiAuthorization
import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  SetCatalogAndNamespace,
  SubqueryAlias
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, TiContext}
import org.slf4j.LoggerFactory

/**
 * Only work for table v2(catalog plugin)
 */
case class TiAuthorizationRule(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {

  protected lazy val meta: MetaManager = tiContext.meta
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  private lazy val tiAuthorization: Option[TiAuthorization] = tiContext.tiAuthorization
  private val logger = LoggerFactory.getLogger(getClass.getName)

  protected def checkForAuth: PartialFunction[LogicalPlan, LogicalPlan] = {
    case sa @ SubqueryAlias(identifier, child) =>
      if (identifier.qualifier.nonEmpty) {
        TiAuthorization.authorizeForSelect(
          identifier.name,
          identifier.qualifier.last,
          tiAuthorization)
      }
      sa
    case sd @ SetCatalogAndNamespace(catalogManager, catalogName, namespace) =>
      if (namespace.isDefined) {
        namespace.get
          .foreach(TiAuthorization.authorizeForSetDatabase(_, tiAuthorization))
      }
      sd
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (TiAuthorization.enableAuth) {
      plan transformUp checkForAuth
    } else {
      plan
    }
}
