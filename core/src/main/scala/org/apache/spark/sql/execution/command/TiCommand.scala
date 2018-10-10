package org.apache.spark.sql.execution.command

import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog

trait TiCommand {
  val tiContext: TiContext
  def tiCatalog: TiSessionCatalog = tiContext.tiCatalog
}