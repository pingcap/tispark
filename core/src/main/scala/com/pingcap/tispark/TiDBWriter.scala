package com.pingcap.tispark

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql._

object TiDBWriter {

  def write(df: DataFrame,
            sqlContext: SQLContext,
            saveMode: SaveMode,
            options: TiDBOptions): Unit = {
    val sparkSession = sqlContext.sparkSession

    TiExtensions.getTiContext(sparkSession) match {
      case Some(tiContext) =>
        val conn = TiDBUtils.createConnectionFactory(options.url)()

        try {
          val tableExists = TiDBUtils.tableExists(conn, options)
          if (tableExists) {
            saveMode match {
              case SaveMode.Append =>
                TiBatchWrite.writeToTiDB(df, tiContext, options)

              case _ =>
                throw new TiBatchWriteException(
                  s"SaveMode: $saveMode is not supported. TiSpark only support SaveMode.Append."
                )
            }
          } else {
            throw new TiBatchWriteException(
              s"table `${options.database}`.`${options.table}` does not exists!"
            )
            // TiDBUtils.createTable(conn, df, options, tiContext)
            // TiDBUtils.saveTable(tiContext, df, Some(df.schema), options)
          }
        } finally {
          conn.close()
        }
      case None => throw new TiBatchWriteException("TiExtensions is disable!")
    }

  }
}
