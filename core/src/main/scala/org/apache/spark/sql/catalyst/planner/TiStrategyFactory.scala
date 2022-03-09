package org.apache.spark.sql.catalyst.planner

import com.pingcap.tispark.utils.ReflectionUtil
import org.apache.spark.sql.{SparkSession, Strategy, TiContext, TiExtensions}

class TiStrategyFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Strategy) {
  override def apply(sparkSession: SparkSession): Strategy = {
    TiExtensions.validateCatalog(sparkSession)
    ReflectionUtil.newTiStrategy(getOrCreateTiContext, sparkSession)
  }
}
