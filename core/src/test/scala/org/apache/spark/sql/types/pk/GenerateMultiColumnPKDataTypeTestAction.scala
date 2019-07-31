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
    val cnt = if (pk.size == 1) {
      if (pk.head == "col_boolean") {
        2
      } else if (pk.head == "col_tinyint") {
        256
      } else {
        rowCount
      }
    } else {
      rowCount
    }
    randomDataGenerator(schema, cnt, dataTypeTestDir, r)
  }

  def addDescription(dataType: ReflectedDataType): (ReflectedDataType, String, String) = {
    val len = genLen(dataType)
    (dataType, len, "")
  }

  def addDescriptionNotNullable(
    dataType: ReflectedDataType
  ): (ReflectedDataType, String, String) = {
    val len = genLen(dataType)
    (dataType, len, "not null")
  }

  override def init(): Unit = {
    val dataTypesWithDescription = dataTypes.map { addDescription }
    for (i <- 5 until 7) { //dataTypes.indices) {
      for (j <- 5 until 7) { //dataTypes.indices) {
        val dt = List(dataTypes(i), dataTypes(j)) ++ dataTypes
        val tableName = getTableName(dt.map(getTypeName): _*)
        val schema = genSchema(
          tableName,
          List(addDescriptionNotNullable(dataTypes(i)), addDescriptionNotNullable(dataTypes(j))) ++ dataTypesWithDescription
        )
        val data = genData(schema)
        data.save()
      }
    }
  }
}
