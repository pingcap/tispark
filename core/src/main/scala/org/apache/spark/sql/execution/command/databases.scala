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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession, TiContext}
import org.apache.spark.sql.catalyst.plans.logical.{SetCatalogAndNamespace, ShowNamespaces}

/**
 * CHECK Spark [[org.apache.spark.sql.catalyst.plans.logical.SetCatalogAndNamespace]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original SetCatalogAndNamespace
 */
case class TiSetDatabaseCommand(tiContext: TiContext, delegate: SetCatalogAndNamespace)
    extends TiCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    tiCatalog.setCurrentDatabase(delegate.namespace.get.head)
    Seq.empty[Row]
  }
}

/**
 * CHECK Spark [[org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original ShowDatabasesCommand
 */
case class TiShowDatabasesCommand(tiContext: TiContext, delegate: ShowNamespaces)
    extends TiCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databases =
      // Not leveraging catalog-specific db pattern, at least Hive and Spark behave different than each other.
      delegate.pattern
        .map(tiCatalog.listDatabases)
        .getOrElse(tiCatalog.listDatabases())
    databases.map { d =>
      Row(d)
    }
  }
}
