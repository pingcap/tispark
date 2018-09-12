package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.analysis.EmptyFunctionRegistry

class TiConcreteSessionCatalog(tiContext: TiContext)(tiExternalCatalog: TiExternalCatalog)
    extends SessionCatalog(
      tiExternalCatalog,
      EmptyFunctionRegistry,
      tiContext.sqlContext.conf
    )
    with TiSessionCatalog {

  override def catalogOf(database: Option[String]): Option[SessionCatalog] = {
    val db = database.getOrElse(getCurrentDatabase)
    if (databaseExists(db))
      Some(this)
    else
      None
  }
}
