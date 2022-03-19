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

package com.pingcap.tispark

import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression, SortOrder}

object SparkWrapper {
  def getVersion: String = {
    "SparkWrapper-3.0"
  }

  def newAlias(child: Expression, name: String): Alias = {
    Alias(child, name)()
  }

  def newAlias(child: Expression, name: String, exprId: ExprId): Alias = {
    Alias(child, name)(exprId = exprId)
  }

  def trimNonTopLevelAliases(e: Expression): Expression = {
    CleanupAliases.trimNonTopLevelAliases(e)
  }

  def copySortOrder(sortOrder: SortOrder, child: Expression): SortOrder = {
    sortOrder.copy(child = child)
  }
}
