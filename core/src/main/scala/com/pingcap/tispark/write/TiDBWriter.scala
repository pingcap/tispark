/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.write

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tispark.TiDBUtils
import org.apache.spark.sql._

object TiDBWriter {

  def write(
      df: DataFrame,
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
                TiBatchWrite.write(df, tiContext, options)

              case _ =>
                throw new TiBatchWriteException(
                  s"SaveMode: $saveMode is not supported. TiSpark only support SaveMode.Append.")
            }
          } else {
            throw new TiBatchWriteException(
              s"table `${options.database}`.`${options.table}` does not exists!")
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
