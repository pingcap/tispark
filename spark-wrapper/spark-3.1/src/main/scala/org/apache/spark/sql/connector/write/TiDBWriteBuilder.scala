package org.apache.spark.sql.connector.write

import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

case class TiDBWriteBuilder(
    info: LogicalWriteInfo,
    tiDBOptions: TiDBOptions,
    sqlContext: SQLContext)
    extends V1WriteBuilder {
  def buildForV1Write: InsertableRelation = {
    new InsertableRelation {
      val schema = info.schema()
      override def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val df = sqlContext.sparkSession.createDataFrame(data.toJavaRDD, schema)
        df.write
          .format("tidb")
          .options(tiDBOptions.parameters)
          .option(TiDBOptions.TIDB_DATABASE, tiDBOptions.database)
          .option(TiDBOptions.TIDB_TABLE, tiDBOptions.table)
          .mode(SaveMode.Append)
          .save()
      }
    }
  }
}
