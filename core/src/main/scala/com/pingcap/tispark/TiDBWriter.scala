package com.pingcap.tispark

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, TiContext}

object TiDBWriter {

  def write(df: DataFrame,
            sqlContext: SQLContext,
            saveMode: SaveMode,
            parameters: Map[String, String]): Unit = {
    val options = new TiDBOptions(parameters)
    val tiContext = new TiContext(sqlContext.sparkSession, Some(options))
    val conn = TiDBUtils.createConnectionFactory(options)()

    try {
      val tableExists = TiDBUtils.tableExists(conn, options)
      if (tableExists) {
        saveMode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && TiDBUtils
                  .isCascadingTruncateTable(options.url)
                  .contains(false)) {
              // In this case, we should truncate table and then load.
              TiDBUtils.truncateTable(conn, options, tiContext)
              val tableSchema = TiDBUtils.getSchemaOption(conn, options)
              TiDBUtils.saveTable(tiContext, df, tableSchema, options)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              TiDBUtils.dropTable(conn, options, tiContext)
              TiDBUtils.createTable(conn, df, options, tiContext)
              TiDBUtils.saveTable(tiContext, df, Some(df.schema), options)
            }

          case SaveMode.Append =>
            val tableSchema = TiDBUtils.getSchemaOption(conn, options)
            TiDBUtils.saveTable(tiContext, df, tableSchema, options)

          case SaveMode.ErrorIfExists =>
            throw new Exception(
              s"Table or view '${options.dbtable}' already exists. SaveMode: ErrorIfExists."
            )

          case SaveMode.Ignore =>
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
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
