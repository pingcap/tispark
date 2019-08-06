/*
 *
 * Copyright 2019 PingCAP, Inc.
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
 *
 */

package org.apache.spark.sql

import org.apache.spark.sql.test.generator.DataType.ReflectedDataType

trait BaseTestGenerationSpec {

  protected val rowCount: Int

  protected val preDescription: String = "Generating Data for "

  protected var cols: List[ReflectedDataType] = List.empty[ReflectedDataType]

  def getTableName(dataTypes: String*): String

  def getTableNameWithDesc(desc: String, dataTypes: String*): String

  def getColumnName(dataType: String): String = s"col_$dataType"

  def getColumnNameByOffset(offset: Int): String = {
    assert(
      cols.size > offset,
      "column length incorrect, maybe `cols` is not initialized correctly?"
    )
    val dataType = cols(offset)
    val suffix = if (cols.count(_ == dataType) > 1) {
      var cnt = 0
      for (i <- 0 until offset) {
        if (cols(i) == dataType) {
          cnt += 1
        }
      }
      s"$cnt"
    } else {
      ""
    }
    s"${getColumnName(dataType.toString)}$suffix"
  }

  def getIndexName(dataTypes: String*): String =
    s"idx_${dataTypes.map(getColumnName).mkString("_")}"

  def getIndexNameByOffset(offsets: Int*): String =
    s"idx_${offsets.map(getColumnNameByOffset).mkString("_")}"

}
