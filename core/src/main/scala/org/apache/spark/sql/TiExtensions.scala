package org.apache.spark.sql

import org.apache.spark.sql.extensions.{TiDDLRule, TiParser, TiResolutionRule}

import scala.collection.mutable

class TiExtensions extends (SparkSessionExtensions => Unit) {
  private val tiContextMap = mutable.HashMap.empty[SparkSession, TiContext]

  private def getOrCreateTiContext(sparkSession: SparkSession): TiContext = synchronized {
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
  def enabled(sparkSession: SparkSession): Boolean = getTiContext(sparkSession).isDefined

  def getTiContext(sparkSession: SparkSession): Option[TiContext] = {
    if (sparkSession.sessionState.planner.extraPlanningStrategies.nonEmpty &&
        sparkSession.sessionState.planner.extraPlanningStrategies.head
          .isInstanceOf[TiStrategy]) {
      Some(
        sparkSession.sessionState.planner.extraPlanningStrategies.head
          .asInstanceOf[TiStrategy]
          .getOrCreateTiContext(sparkSession)
      )
    } else if (sparkSession.experimental.extraStrategies.nonEmpty &&
               sparkSession.experimental.extraStrategies.head.isInstanceOf[TiStrategy]) {
      Some(
        sparkSession.experimental.extraStrategies.head
          .asInstanceOf[TiStrategy]
          .getOrCreateTiContext(sparkSession)
      )
    } else {
      None
    }
  }
}
