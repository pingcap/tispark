package com.pingcap.tispark

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, TiContext}

object TiDBWriter {

  def write(df: DataFrame,
            sqlContext: SQLContext,
            saveMode: SaveMode,
            options: TiDBOptions): Unit = {
    val tiContext = new TiContext(sqlContext.sparkSession, Some(options))
    // adding this in order to resolve drop-and-create table with same name but different table id
    // problem.
    tiContext.tiSession.getCatalog.reloadCache(true)
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
  }
}
