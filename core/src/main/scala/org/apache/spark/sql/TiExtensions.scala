package org.apache.spark.sql

import org.apache.spark.sql.extensions.TiResolutionRule

class TiExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectResolutionRule(TiResolutionRule(TiExtensions.getOrCreateTiContext))
    e.injectPlannerStrategy(TiStrategy(TiExtensions.getOrCreateTiContext))
  }
}

object TiExtensions {
  private var tiContext: TiContext = _

  def getOrCreateTiContext(sparkSession: SparkSession): TiContext = {
    if (tiContext == null) {
      tiContext = new TiContext(sparkSession)
    }
    tiContext
  }
}