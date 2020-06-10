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

import org.apache.spark.sql.test.generator.DataType.{getBaseType, DECIMAL, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator.{
  getDecimal,
  getLength,
  isCharOrBinary,
  isVarString
}

trait BaseTestGenerationSpec {

  protected def rowCount: Int

  protected val preDescription: String = "Generating Data for "

  protected var cols: List[ReflectedDataType] = _

  protected var canTestTiFlash: Boolean = false

  def getTableName(dataTypes: String*): String

  def getTableNameWithDesc(desc: String, dataTypes: String*): String

  def getIndexName(dataTypes: String*): String =
    s"idx_${dataTypes.map(getColumnName).mkString("_")}"

  def getColumnName(dataType: String): String = s"col_$dataType"

  def getIndexNameByOffset(offsets: Int*): String =
    s"idx_${offsets.map(getColumnNameByOffset).mkString("_")}"

  def getColumnNameByOffset(offset: Int): String = {
    require(cols != null)
    assert(
      cols.lengthCompare(offset) > 0,
      s"column length incorrect ${cols.size} <= $offset, maybe `cols` is not initialized correctly?")
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

  def getTypeLength(dataType: ReflectedDataType): String = {
    val baseType = getBaseType(dataType)
    val length = getLength(baseType)
    dataType match {
      case DECIMAL => s"$length,${getDecimal(baseType)}"
      case _ if isVarString(dataType) => s"$length"
      case _ if isCharOrBinary(dataType) => "10"
      case _ => ""
    }
  }

}
