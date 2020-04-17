package org.apache.spark.sql

import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.extensions.{TiDDLRule, TiParser, TiResolutionRule}

import scala.collection.mutable

class TiExtensions extends (SparkSessionExtensions => Unit) {

  private var tiContext: TiContext = _

  def getOrCreateTiContext(sparkSession: SparkSession): TiContext = {
    TiUtil.registerUDFs(sparkSession)

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
