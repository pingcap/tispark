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

import org.apache.spark.sql.{Row, SparkSession, TiContext}
import org.apache.spark.sql.catalyst.catalog.{
  CatalogTable,
  CatalogTableType,
  CatalogUtils,
  TiSessionCatalog
}

case class TiCreateTableLikeCommand(tiContext: TiContext, delegate: CreateTableLikeCommand)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = tiContext.tiCatalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(delegate.sourceTable)

    val newProvider = if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else if (catalog
        .catalogOf(sourceTableDesc.identifier.database)
        .exists(_.isInstanceOf[TiSessionCatalog])) {
      // TODO: use tikv datasource
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else {
      sourceTableDesc.provider
    }

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType =
      if (delegate.fileFormat.locationUri.isEmpty) CatalogTableType.MANAGED
      else CatalogTableType.EXTERNAL

    val newTableDesc =
      CatalogTable(
        identifier = delegate.targetTable,
        tableType = tblType,
        storage = sourceTableDesc.storage
          .copy(locationUri = delegate.fileFormat.locationUri),
        schema = sourceTableDesc.schema,
        provider = newProvider,
        partitionColumnNames = sourceTableDesc.partitionColumnNames,
        bucketSpec = sourceTableDesc.bucketSpec)

    catalog.createTable(newTableDesc, delegate.ifNotExists)
    Seq.empty[Row]
  }
}
