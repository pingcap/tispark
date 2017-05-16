package org.apache.spark.sql

import com.pingcap.tispark.{TiDBRelation, TiOptions}
import org.apache.spark.internal.Logging


class TiContext (val session: SparkSession) extends Serializable with Logging {
  val sqlContext = session.sqlContext
  def tidbTable(
                 tiAddresses: List[String],
                 dbName: String,
                 tableName: String
               ): DataFrame = {
    logDebug("Creating tiContext...")
    val tiRelation = TiDBRelation(new TiOptions(tiAddresses, dbName, tableName))(sqlContext)
    session.experimental.extraStrategies ++= Seq(new TiStrategies(sqlContext))
    sqlContext.baseRelationToDataFrame(tiRelation)
  }
}
