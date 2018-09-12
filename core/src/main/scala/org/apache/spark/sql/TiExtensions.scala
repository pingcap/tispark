package org.apache.spark.sql

import com.pingcap.tispark.TiSparkVersion
import com.pingcap.tispark.listener.CacheInvalidateListener
import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.sql.extensions.{TiDDLRule, TiParser, TiResolutionRule}

class TiExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(TiParser(TiExtensions.getOrCreateTiContext))
    e.injectResolutionRule(TiDDLRule)
    e.injectResolutionRule(TiResolutionRule(TiExtensions.getOrCreateTiContext))
    e.injectPlannerStrategy(TiStrategy(TiExtensions.getOrCreateTiContext))
  }
}

object TiExtensions {
  private var tiContext: TiContext = _

  def getOrCreateTiContext(sparkSession: SparkSession): TiContext = {
    if (tiContext == null) {
      tiContext = new TiContext(sparkSession)
      StatisticsManager.initStatisticsManager(tiContext.tiSession, sparkSession)
      sparkSession.udf.register("ti_version", () => TiSparkVersion.version)
      CacheInvalidateListener
        .initCacheListener(sparkSession.sparkContext, tiContext.tiSession.getRegionManager)
      tiContext.tiSession.injectCallBackFunc(CacheInvalidateListener.getInstance())
    }
    tiContext
  }
}
