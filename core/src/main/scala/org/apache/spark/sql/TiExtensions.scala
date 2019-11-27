package org.apache.spark.sql

import org.apache.spark.sql.extensions.{TiDDLRule, TiParser, TiResolutionRule}

import scala.collection.mutable

class TiExtensions extends (SparkSessionExtensions => Unit) {
  private val tiContextMap = mutable.HashMap.empty[SparkSession, TiContext]

  def getOrCreateTiContext(sparkSession: SparkSession): TiContext = synchronized {
    tiContextMap.get(sparkSession) match {
      case Some(tiContext) => tiContext
      case None            =>
        // TODO: make Meta and RegionManager independent to sparkSession
        val tiContext = new TiContext(sparkSession)
        tiContextMap.put(sparkSession, tiContext)
        tiContext
    }
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
          tiExtensions.apply(sparkSession.extensions)
        }
      }
    }
    tiExtensions
  }

  def reset(): Unit = tiExtensions = null
}
