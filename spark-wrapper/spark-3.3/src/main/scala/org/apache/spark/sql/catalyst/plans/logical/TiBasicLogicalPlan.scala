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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.logical

import com.pingcap.tispark.auth.TiAuthorization
import org.apache.spark.sql.catalyst.analysis.{ResolvedDBObjectName, ResolvedNamespace}

object TiBasicLogicalPlan {
  def verifyAuthorizationRule(
      logicalPlan: LogicalPlan,
      tiAuthorization: Option[TiAuthorization]): LogicalPlan = {
    logicalPlan match {
      case st @ SetCatalogAndNamespace(namespace) =>
        namespace match {
          case ResolvedNamespace(catalog, ns) =>
            if (catalog.name().equals("tidb_catalog")) {
              ns.foreach(TiAuthorization.authorizeForSetDatabase(_, tiAuthorization))
            }
            st
          case ResolvedDBObjectName(catalog, nameParts) =>
            if (catalog.name().equals("tidb_catalog")) {
              nameParts.foreach(TiAuthorization.authorizeForSetDatabase(_, tiAuthorization))
            }
            st
        }
    }
  }

}
