package com.pingcap.tispark

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, TiContext}

object TiDBWriter {

  def write(df: DataFrame,
            sqlContext: SQLContext,
            saveMode: SaveMode,
            options: TiDBOptions): Unit = {
    val tiContext = new TiContext(sqlContext.sparkSession, Some(options))
    val conn = TiDBUtils.createConnectionFactory(options)()

    try {
      val tableExists = TiDBUtils.tableExists(conn, options)
      if (tableExists) {
        saveMode match {
          case SaveMode.Append =>
            val tableSchema = TiDBUtils.getSchemaOption(conn, options)
            TiDBUtils.saveTable(tiContext, df, tableSchema, options)

          case _ =>
            throw new TiBatchWriteException(
              s"SaveMode: $saveMode is not supported. TiSpark only support SaveMode.Append."
            )
        }
      } else {
        TiDBUtils.createTable(conn, df, options, tiContext)
        TiDBUtils.saveTable(tiContext, df, Some(df.schema), options)
      }
    } finally {
      conn.close()
    }
  }
}
