package org.apache.spark.sql

import org.apache.spark.sql.extensions.{TiDDLRule, TiParser, TiResolutionRule}

import scala.collection.mutable

class TiExtensions extends (SparkSessionExtensions => Unit) {
  private val tiContextMap: mutable.Map[SparkSession, TiContext] =
    new mutable.HashMap[SparkSession, TiContext]

  def getOrCreateTiContext(sparkSession: SparkSession): TiContext =
    synchronized {
      tiContextMap.getOrElseUpdate(sparkSession, new TiContext(sparkSession))
    }

  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(TiParser(getOrCreateTiContext))
    e.injectResolutionRule(TiDDLRule(getOrCreateTiContext))
    e.injectResolutionRule(TiResolutionRule(getOrCreateTiContext))
    e.injectPlannerStrategy(TiStrategy(getOrCreateTiContext))
  }
}

object TiExtensions {
  private var tiExtensions: TiExtensions = _

  def getInstance(sparkSession: SparkSession): TiExtensions = {
    if (tiExtensions == null) {
      synchronized {
        if (tiExtensions == null) {
          tiExtensions = new TiExtensions
        }
      }
    }
    tiExtensions
  }

  def reset(): Unit = tiExtensions = null
}
