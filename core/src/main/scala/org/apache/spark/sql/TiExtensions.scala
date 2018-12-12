package org.apache.spark.sql

import org.apache.spark.sql.extensions.{TiDDLRule, TiParser, TiResolutionRule}

import scala.collection.mutable

class TiExtensions extends (SparkSessionExtensions => Unit) {
  private var tiContext: TiContext = _

  def getOrCreateTiContext(sparkSession: SparkSession): TiContext = {
    if (tiContext == null) {
      tiContext = new TiContext(sparkSession)
    }
    tiContext
  }

  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(TiParser(getOrCreateTiContext))
    e.injectResolutionRule(TiDDLRule(getOrCreateTiContext))
    e.injectResolutionRule(TiResolutionRule(getOrCreateTiContext))
    e.injectPlannerStrategy(TiStrategy(getOrCreateTiContext))
  }
}

object TiExtensions {
  private val tiExtensionsMap: mutable.Map[SparkSession, TiExtensions] =
    new mutable.HashMap[SparkSession, TiExtensions]

  def getInstance(sparkSession: SparkSession): TiExtensions =
    synchronized {
      tiExtensionsMap.getOrElseUpdate(sparkSession, {
        val tiExtensions = new TiExtensions
        tiExtensions.apply(sparkSession.extensions)
        tiExtensions
      })
    }
}
