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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EmptyFunctionRegistry

class TiConcreteSessionCatalog(val tiContext: TiContext)(tiExternalCatalog: ExternalCatalog)
    extends SessionCatalog(tiExternalCatalog, EmptyFunctionRegistry, tiContext.sqlContext.conf)
    with TiSessionCatalog {

  override def catalogOf(database: Option[String]): Option[SessionCatalog] = {
    val db = database.getOrElse(getCurrentDatabase)
    if (databaseExists(db))
      Some(this)
    else
      None
  }

  override def databaseExists(db: String): Boolean = tiExternalCatalog.databaseExists(db)

  override def tableExists(name: TableIdentifier): Boolean =
    tiExternalCatalog.tableExists(name.database.getOrElse(getCurrentDatabase), name.table)

}
