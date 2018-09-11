package org.apache.spark.sql

import org.apache.spark.sql.extensions.TiResolutionRule

class TiExtensions extends (SparkSessionExtensions => Unit) {
  var tiContext: TiContext = _

  def getOrCreateTiContext(sparkSession: SparkSession): TiContext = {
    if (tiContext == null) {
      tiContext = new TiContext(sparkSession)
    }
    tiContext
  }

  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectResolutionRule(TiResolutionRule(getOrCreateTiContext))
    e.injectPlannerStrategy(TiStrategy(getOrCreateTiContext))
  }
}