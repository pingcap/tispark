/*
 * Copyright 2019 PingCAP, Inc.
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

<<<<<<< HEAD
import org.apache.spark.sql.catalyst.catalog.TiCatalog
=======
>>>>>>> aa7b0801baa0661bbb1dd6f4ba667ac11f17257c
import org.apache.spark.sql.{Row, SparkSession, TiContext}
import org.apache.spark.sql.catalyst.plans.logical.{SetCatalogAndNamespace, ShowNamespaces}

/**
 * CHECK Spark [[org.apache.spark.sql.catalyst.plans.logical.SetCatalogAndNamespace]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original SetCatalogAndNamespace
 */
case class TiSetDatabaseCommand(tiContext: TiContext, delegate: SetCatalogAndNamespace)
    extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    tiContext.tiCatalog.setCurrentDatabase(delegate.namespace.get.head)
    Seq.empty[Row]
  }
}

<<<<<<< HEAD
case class TiSetDatabaseCommandV2(tiContext: TiContext, delegate: SetCatalogAndNamespace)
    extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    // The catalog is updated first because CatalogManager resets the current namespace
    // when the current catalog is set.
    val catalogManager = tiContext.sparkSession.sessionState.analyzer.catalogManager
    val previousCatalog = catalogManager.currentCatalog
    delegate.catalogName.foreach(catalogManager.setCurrentCatalog)
    delegate.namespace.foreach(ns => catalogManager.setCurrentNamespace(ns.toArray))

    // record current namespace for tiCatalog, this is a work around for the
    // problem described in https://issues.apache.org/jira/browse/SPARK-30014
    catalogManager.currentCatalog match {
      case tiCatalog: TiCatalog =>
        delegate.namespace.foreach(ns => tiCatalog.setCurrentNamespace(Some(ns.toArray)))
      case _ =>
    }

    // reset the current namespace to None if the previous tiCatalog is not current
    // catalog anymore
    previousCatalog match {
      case tiCatalog: TiCatalog =>
        if (tiCatalog.name() != delegate.catalogName.getOrElse("UNDEFINED"))
          tiCatalog.setCurrentNamespace(None)
      case _ =>
    }
    Seq.empty
  }
}

=======
>>>>>>> aa7b0801baa0661bbb1dd6f4ba667ac11f17257c
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
