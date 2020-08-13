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

package org.apache.spark.sql.test.generator

import org.apache.spark.sql.test.generator.DataType.{ReflectedDataType, getBaseType}
import org.apache.spark.sql.test.generator.TestDataGenerator.{getDecimal, getLength}

trait IndexColumn {
  def id: Int
  def getId: Int = id - 1
  def getLength: Integer = null
}

case class DefaultColumn(id: Int) extends IndexColumn {}

case class PrefixColumn(id: Int, length: Int) extends IndexColumn {
  override def getLength: Integer = length
}

case class ColumnInfo(
    columnName: String,
    dataType: ReflectedDataType,
    length: (Integer, Integer),
    desc: String) {

  val isPrimaryKey: Boolean = desc.contains("primary key")
  val nullable: Boolean = !isPrimaryKey && !desc.contains("not null")
  val breakDown: Array[String] = desc.split(" ")
  val unsigned: Boolean = breakDown.contains("unsigned")
  val noDefault: Boolean = !breakDown.contains("default")
  val isUnique: Boolean = breakDown.contains("unique")
  val default: String = {
    if (noDefault) {
      null
    } else {
      val idx = breakDown.indexOf("default")
      assert(idx >= 0)
      if (idx == breakDown.length - 1) {
        null
      } else {
        breakDown(idx + 1)
      }
    }
  }
  val (len, decimal): (Long, Int) = {
    val baseType = getBaseType(dataType)
    if (length._1 == null) {
      (getLength(baseType), getDecimal(baseType))
    } else if (length._2 == null) {
      (length._1.toLong, getDecimal(baseType))
    } else {
      (length._1.toLong, length._2)
    }
  }
  val generator: ColumnValueGenerator =
    ColumnValueGenerator(
      dataType,
      len,
      decimal,
      nullable,
      unsigned,
      noDefault,
      default,
      isPrimaryKey,
      isUnique)

  {
    // validation
    import org.apache.spark.sql.test.generator.TestDataGenerator._
    if (isVarString(dataType) && len == -1) {
      throw new IllegalArgumentException("Length must be specified for Text and BLOB Types")
    }
  }

  override def toString: String = s"`$columnName` ${generator.toString}"
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

case class IndexInfo(
    indexName: String,
    indexColumns: List[IndexColumnInfo],
    isPrimary: Boolean,
    isUnique: Boolean) {
  override def toString: String = {
    val indexColumnString = indexColumns.mkString("(", ",", ")")
    if (isPrimary) {
      s"PRIMARY KEY $indexColumnString"
    } else if (isUnique) {
      s"UNIQUE KEY $indexColumnString"
    } else {
      s"KEY `$indexName`$indexColumnString"
    }
  }
}
