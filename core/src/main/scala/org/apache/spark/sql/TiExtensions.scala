package org.apache.spark.sql

import com.pingcap.tispark.TiConfigConst
import com.pingcap.tispark.utils.ReflectionUtil

class TiExtensions extends (SparkSessionExtensions => Unit) {
  private var tiContext: TiContext = _

  def getOrCreateTiContext(sparkSession: SparkSession): TiContext = {
    if (tiContext == null) {
      tiContext = new TiContext(sparkSession)
    }
    tiContext
  }

  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(ReflectionUtil.newTiParser(getOrCreateTiContext))
    e.injectResolutionRule(ReflectionUtil.newTiDDLRule(getOrCreateTiContext))
    e.injectResolutionRule(ReflectionUtil.newTiResolutionRule(getOrCreateTiContext))
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

  def enabled(): Boolean = tiExtensions != null

  def reset(): Unit = tiExtensions = null

  def catalogPluginMode(sparkSession: SparkSession): Boolean =
    sparkSession.sparkContext.conf.get(TiConfigConst.USE_CATALOG_PLUGIN, "false").toBoolean
}
