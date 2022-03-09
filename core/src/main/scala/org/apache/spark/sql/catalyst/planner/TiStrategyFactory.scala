package org.apache.spark.sql.catalyst.planner

import com.pingcap.tispark.TiSparkInfo
import com.pingcap.tispark.utils.ReflectionUtil
import org.apache.spark.sql.{SparkSession, Strategy, TiContext}

class TiStrategyFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Strategy) {
  override def apply(sparkSession: SparkSession): Strategy = {
    TiStrategy(getOrCreateTiContext)(sparkSession)

    //ReflectionUtil.newTiStrategy(getOrCreateTiContext, sparkSession)
  }
}
