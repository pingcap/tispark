/*
 *
 * Copyright 2017 PingCAP, Inc.
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

package org.apache.spark.sql.types

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.generator.{Data, Index, Schema}
import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator.{getDecimal, getLength, isCharOrBinary, isVarString, randomDataGenerator, schemaGenerator}

import scala.util.Random

class BaseDataTypeTest extends BaseTiSparkTest {

  val dataTypes: List[ReflectedDataType] = List(TINYINT)
  val unsignedDataTypes: List[ReflectedDataType] = List(TINYINT)
  val dataTypeTestDir = "dataType-test"
  val database = "data_type_test"
  val testDesc = "Base test for data types"
  val r: Random = new Random(1234)
  private val extraDesc = "unsigned"

  def simpleSelect(dbName: String, dataType: String): Unit = {
    setCurrentDatabase(dbName)
    val tblName = getTableName(dataType)
    val query = s"select ${getColumnName(dataType)} from $tblName"
    runTest(query)
  }

  def simpleSelect(dbName: String, dataType: String, desc: String): Unit = {
    setCurrentDatabase(dbName)
    val tblName = getTableName(dataType, desc)
    val query = s"select ${getColumnName(dataType)} from $tblName"
    runTest(query)
  }

  def getTableName(dataType: String): String = s"test_$dataType"

  def getTableName(dataType: String, desc: String): String = s"test_${desc}_$dataType"

  def getColumnName(dataType: String): String = s"col_$dataType"

  def getIndexName(dataType: String): String = s"idx_$dataType"

  def genSchema(dataType: ReflectedDataType,
                tableName: String,
                len: String,
                desc: String): Schema = {
    schemaGenerator(
      database,
      tableName,
      r,
      List(
        (dataType, len, desc)
      ),
      List.empty[Index]
    )
  }

  def genData(schema: Schema): Data = randomDataGenerator(schema, 20, dataTypeTestDir, r)

  def genLen(dataType: ReflectedDataType): String = {
    val baseType = getBaseType(dataType)
    val length = getLength(baseType)
    logger.warn(dataType + " " + length)
    dataType match {
      case DECIMAL                       => s"$length,${getDecimal(baseType)}"
      case _ if isVarString(dataType)    => s"$length"
      case _ if isCharOrBinary(dataType) => "10"
      case _                             => ""
    }
  }

  def init(): Unit = {
    for (dataType <- dataTypes) {
      val typeName = getTypeName(dataType)
      val len = genLen(dataType)
      val tableName = getTableName(typeName)
      val schema = genSchema(dataType, tableName, len, "")
      val data = genData(schema)
      data.save()
    }
    for (dataType <- unsignedDataTypes) {
      val typeName = getTypeName(dataType)
      val len = genLen(dataType)
      val tableName = getTableName(typeName, extraDesc)
      val schema = genSchema(dataType, tableName, len, extraDesc)
      val data = genData(schema)
      data.save()
    }
  }

  def run(): Unit = {
    SharedSQLContext.init()
    if (generateData) {
      logger.info("generating data")
      init()
    }
    val preDescription = if (generateData) "Generating Data for " else ""
    for (dataType <- dataTypes) {
      val typeName = getTypeName(dataType)
      test(s"${preDescription}Test $typeName - $testDesc") {
        if (generateData) {
          loadSQLFile(dataTypeTestDir, getTableName(typeName))
        } else {
          simpleSelect(database, typeName)
        }
      }
    }
    for (dataType <- unsignedDataTypes) {
      val typeName = getTypeName(dataType)
      test(s"${preDescription}Test $extraDesc $typeName - $testDesc") {
        if (generateData) {
          loadSQLFile(dataTypeTestDir, getTableName(typeName, extraDesc))
        } else {
          simpleSelect(database, typeName, extraDesc)
        }
      }
    }
  }
}
