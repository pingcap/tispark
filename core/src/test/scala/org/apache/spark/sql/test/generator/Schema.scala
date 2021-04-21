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

import org.apache.spark.sql.test.generator.DataType._

/**
 * Case class for Schema of TiDB table
 *
 * @param database     database name
 * @param tableName    table name
 * @param columnNames  name of each column
 * @param columnDesc   in the format of (columnName, (dataType, lengthDescriptions, otherDescriptions))
 *                     lengthDescriptions in the format of (M, D) describes detailed information for data type
 * @param indexColumns in the format of (indexName, {list of column names})
 */
case class Schema(
    database: String,
    tableName: String,
    columnNames: List[String],
    columnDesc: Map[String, (ReflectedDataType, (Integer, Integer), String)],
    indexColumns: Map[String, (List[(String, Integer)], Boolean, Boolean)]) {

  // validations
  assert(columnDesc.size == columnNames.size, "columnDesc size not equal to column name size")
  assert(columnNames.forall(columnDesc.contains), "column desc not present for some columns")

  val indexInfo: List[IndexInfo] = indexColumns.map { idx =>
    IndexInfo(
      idx._1,
      idx._2._1.map { x =>
        IndexColumnInfo(x._1, x._2)
      },
      idx._2._2,
      idx._2._3)
  }.toList

  assert(indexInfo.count(_.isPrimary) <= 1, "more than one primary key exist in schema")

  val pkIndexInfo: List[IndexInfo] = indexInfo.filter(_.isPrimary)
  val pkColumnName: String = if (pkIndexInfo.isEmpty) {
    ""
  } else {
    pkIndexInfo.head.indexColumns.map(_.column).mkString(",")
  }

  val uniqueIndexInfo: List[IndexInfo] = indexInfo.filter(_.isUnique)
  val uniqueColumnNames: List[String] = uniqueIndexInfo.map { indexInfo =>
    indexInfo.indexColumns.map(_.column).mkString(",")
  }

  val columnInfo: List[ColumnInfo] = columnNames.map { col =>
    val x = columnDesc(col)
    if (col == pkColumnName) {
      ColumnInfo(col, x._1, x._2, x._3 + " primary key")
    } else if (uniqueColumnNames.contains(col)) {
      ColumnInfo(col, x._1, x._2, x._3 + " unique key")
    } else {
      ColumnInfo(col, x._1, x._2, x._3)
    }
  }

  // column info to string
  private val columns: List[String] = columnInfo.map(_.toString)
  private val keys: List[String] = indexInfo.map(_.toString)

  override def toString: String = {
    toString(isClusteredIndex = false)
  }

  def toString(isClusteredIndex: Boolean): String = {
    val clusteredIndexStr = if (isClusteredIndex) " /*T![clustered_index] CLUSTERED */" else ""
    val index = if (keys.nonEmpty) {
      keys.mkString(",\n|  ", ",\n|  ", "")
    } else ""
    (s"CREATE TABLE `$database`.`$tableName` (\n|  ".stripMargin +
      columns.mkString(",\n|  ") +
      index + clusteredIndexStr +
      "\n|) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin").stripMargin
  }
}
