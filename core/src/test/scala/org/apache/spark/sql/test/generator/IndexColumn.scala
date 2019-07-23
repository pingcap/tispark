/*
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
 */

package org.apache.spark.sql.test.generator

import org.apache.spark.sql.test.generator.DataType.{getBaseType, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator.{getDecimal, getLength}

trait IndexColumn {
  val id: Int
  def getId: Int = id - 1
  def getLength: Integer = null
}

case class DefaultColumn(id: Int) extends IndexColumn {}

case class PrefixColumn(id: Int, length: Int) extends IndexColumn {
  override def getLength: Integer = length
}

case class ColumnInfo(columnName: String,
                      dataType: ReflectedDataType,
                      length: (Integer, Integer),
                      desc: String) {

  val isPrimaryKey: Boolean = desc.contains("primary key")
  val nullable: Boolean = !isPrimaryKey && !desc.contains("not null")
  val unsigned: Boolean = desc.contains("unsigned")
  val noDefault: Boolean = !desc.contains("default")
  val default: String = {
    if (noDefault) {
      null
    } else {
      val breakDown = desc.split(" ")
      val idx = breakDown.indexOf("default")
      assert(idx >= 0)
      if (idx == breakDown.length - 1) {
        null
      } else {
        breakDown(idx + 1)
      }
    }
  }

  private val baseType = getBaseType(dataType)

  private val (len, decimal): (Long, Int) = if (length._1 == null) {
    (getLength(baseType), getDecimal(baseType))
  } else if (length._2 == null) {
    (length._1.toLong, getDecimal(baseType))
  } else {
    (length._1.toLong, length._2)
  }

  {
    // validation
    import TestDataGenerator._
    if (isVarString(dataType) && len == -1) {
      throw new IllegalArgumentException("Length must be specified for Text and BLOB Types")
    }
  }

  val generator: ValueGenerator =
    ValueGenerator(dataType, len, decimal, nullable, unsigned, noDefault, default, isPrimaryKey)

  override def toString: String = {
    "`" + columnName + "` " + s"${generator.toString}"
  }
}

case class IndexColumnInfo(column: String, length: Integer) {
  override def toString: String = {
    val prefixInfo = if (length == null) {
      ""
    } else {
      s"($length)"
    }
    s"`$column`$prefixInfo"
  }
}

case class IndexInfo(indexName: String, indexColumns: List[IndexColumnInfo], isPrimary: Boolean) {
  override def toString: String = {
    val indexColumnString = indexColumns.mkString("(", ",", ")")
    if (isPrimary) {
      s"PRIMARY KEY $indexColumnString"
    } else {
      s"KEY `$indexName`$indexColumnString"
    }
  }
}
