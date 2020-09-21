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

import java.util.Locale

import org.apache.spark.sql.TiContext
import org.apache.spark.sql.internal.StaticSQLConf

trait TiSessionCatalog extends SessionCatalog {

  protected lazy val globalTempDB: String =
    tiContext.sparkSession.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE).toLowerCase(Locale.ROOT)
  protected def tiContext: TiContext

  /**
   * Returns the catalog in which the database is.
   * Use for upper command to identify the catalog in order to take different actions.
   * @param database database
   * @return respective catalog containing this database,
   *        returns current catalog when database is empty
   */
  def catalogOf(database: Option[String] = None): Option[SessionCatalog]

  def catalogOf(tableIdentifier: Seq[String]): Option[SessionCatalog] = {
    catalogOf(if (tableIdentifier.size == 1) None else Some(tableIdentifier.head))
  }
}
