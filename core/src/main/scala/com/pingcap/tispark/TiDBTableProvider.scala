package com.pingcap.tispark

import com.pingcap.tikv.exception.{TiBatchWriteException, TiClientInternalException}
import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.{SparkSession, TiContext, TiExtensions}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class TiDBTableProvider extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(null, Array.empty[Transform], options.asCaseSensitiveMap()).schema()
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {

    val scalaMap = properties.asScala.toMap
    val mergeOptions = new TiDBOptions(scalaMap)
    val sparkSession = SparkSession.active

    TiExtensions.getTiContext(sparkSession) match {
      case Some(tiContext) =>
        val ts = tiContext.tiSession.getTimestamp
        TiDBTable(
          tiContext.tiSession,
          mergeOptions.getTiTableRef(tiContext.tiConf),
          tiContext.meta,
          ts,
          Some(mergeOptions))(tiContext)
      case None => throw new TiBatchWriteException("TiExtensions is disable!")
    }
  }

}
