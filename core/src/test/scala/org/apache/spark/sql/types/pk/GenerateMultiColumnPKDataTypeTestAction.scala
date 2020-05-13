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

package org.apache.spark.sql.types.pk

import org.apache.spark.sql.test.generator.DataType.{getTypeName, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator.{isStringType, randomDataGenerator, schemaGenerator}
import org.apache.spark.sql.test.generator._
import org.apache.spark.sql.types.GenerateMultiColumnDataTypeTestAction

import scala.collection.mutable
import scala.util.Random

trait GenerateMultiColumnPKDataTypeTestAction extends GenerateMultiColumnDataTypeTestAction {

  override val rowCount: Int = 10

  private val dataTypesWithDescription = dataTypes.map { genDescription }

  private def genIndex(dataTypesWithDescription: List[(ReflectedDataType, String, String)],
                       r: Random): List[Index] = {
    assert(
      dataTypesWithDescription.size >= 2,
      "column size should be at least 2 for multi-column tests"
    )
    val result: mutable.ListBuffer[IndexColumn] = new mutable.ListBuffer[IndexColumn]()
    for (i <- 0 until 2) {
      val d = dataTypesWithDescription(i)._1
      if (isStringType(d)) {
        result += PrefixColumn(i + 1, r.nextInt(4) + 2)
      } else {
        result += DefaultColumn(i + 1)
      }
    }
    List(PrimaryKey(result.toList))
  }

  override def genSchema(
    tableName: String,
    dataTypesWithDescription: List[(ReflectedDataType, String, String)]
  ): Schema = {
    val index = genIndex(dataTypesWithDescription, r)
    schemaGenerator(
      database,
      tableName,
      r,
      dataTypesWithDescription,
      index
    )
  }

  override def genData(schema: Schema): Data = {
    val pk = schema.pkColumnName.split(",", -1)
    assert(
      pk.nonEmpty && pk.head.nonEmpty,
      "Schema incorrect for PK tests, must contain valid PK info"
    )
    val cnt: Int = Math.min(
      (schema.pkIndexInfo.head.indexColumns.map {
        case b if b.column.contains("col_bit")     => if (b.length == null) 2 else 1 << b.length.toInt
        case b if b.column.contains("col_boolean") => 2
        case i if i.column.contains("col_tinyint") => 256
        case _                                     => 500
      }.product + 2) / 3,
      rowCount
    )
    assert(cnt > 0, "row count should be greater than 0")
    randomDataGenerator(schema, cnt, dataTypeTestDir, r)
  }

  def genDescription(dataType: ReflectedDataType): (ReflectedDataType, String, String) = {
    val len = getTypeLength(dataType)
    (dataType, len, "")
  }

  def genDescriptionNotNullable(
    dataType: ReflectedDataType
  ): (ReflectedDataType, String, String) = {
    val len = getTypeLength(dataType)
    (dataType, len, "not null")
  }

  def init(tableName: String, i: Int, j: Int): Unit = {
    val schema = genSchema(
      tableName,
      List(genDescriptionNotNullable(dataTypes(i)), genDescriptionNotNullable(dataTypes(j))) ++ dataTypesWithDescription
    )
    val data = genData(schema)
    setTiFlashReplicaByConfig(data)
  }

  def test(i: Int, j: Int): Unit = {
    cols = List(dataTypes(i), dataTypes(j)) ++ dataTypes
    val tableName = getTableName(cols.map(getTypeName): _*)
    init(tableName, i, j)
    loadTestData(tableName)
    if (canTestTiFlash) {
      // sleep for some time to wait for TiFlash syncing
      Thread.sleep(10 * 1000)
    }
  }
}
