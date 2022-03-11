/*
 *
 * Copyright 2019 PingCAP Inc.
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

import com.pingcap.tispark.test.generator.DataGenerator._
import com.pingcap.tispark.test.generator.DataType.{ReflectedDataType, getTypeName}
import com.pingcap.tispark.test.generator._
import org.apache.spark.sql.types.BaseRandomDataTypeTest

import scala.util.Random

class DataTypePKSuite extends BaseRandomDataTypeTest {
  override protected def rowCount = 10

  private val dataTypes: List[ReflectedDataType] =
    integers ::: decimals ::: doubles ::: stringType

  private val unsignedDataTypes: List[ReflectedDataType] = integers ::: decimals ::: doubles

  override protected val database = "data_type_test_pk"

  private val testDesc = "Test for single PK column data types (and unsigned types)"

  private val extraDescUnsigned = "unsigned"

  override protected def genIndex(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      r: Random): List[List[Index]] = {
    val dataType = dataTypesWithDesc.head._1
    val index = if (isStringType(dataType)) {
      List(PrimaryKey(List(PrefixColumn(1, r.nextInt(4) + 2))))
    } else {
      List(PrimaryKey(List(DefaultColumn(1))))
    }
    List(index)
  }

  private def startTest(schemaAndData: SchemaAndData, typeName: String): Unit = {
    test(s"Test $typeName - $testDesc") {
      loadToDB(schemaAndData)

      setCurrentDatabase(database)
      val tblName = schemaAndData.schema.tableName
      val colName = schemaAndData.schema.columnNames.head
      val query = s"select $colName from $tblName"
      println(query)
      runTest(query)
    }
  }

  private def startUnsignedTest(schemaAndData: SchemaAndData, typeName: String): Unit = {
    test(s"Test $extraDescUnsigned $typeName - $testDesc") {
      loadToDB(schemaAndData)

      setCurrentDatabase(database)
      val tblName = schemaAndData.schema.tableName
      val colName = schemaAndData.schema.columnNames.head
      val query = s"select $colName from $tblName"
      println(query)
      runTest(query, skipJDBC = true)
    }
  }

  private def generateTestCases(): Unit = {
    for (dataType <- dataTypes) {
      val typeName = getTypeName(dataType)
      val dataTypesWithDesc: List[(ReflectedDataType, String, String)] = List(
        genDescription(dataType, NullableType.NotNullable))

      val schemaAndDataList = genSchemaAndData(
        rowCount,
        dataTypesWithDesc,
        database,
        hasTiFlashReplica = enableTiFlashTest)
      schemaAndDataList.foreach { schemaAndData =>
        startTest(schemaAndData, typeName)
      }
    }
    for (dataType <- unsignedDataTypes) {
      val typeName = getTypeName(dataType)
      val dataTypesWithDesc: List[(ReflectedDataType, String, String)] = List(
        genDescription(dataType, NullableType.NotNullable, extraDescUnsigned))
      val schemaAndDataList =
        genSchemaAndData(
          rowCount,
          dataTypesWithDesc,
          database,
          hasTiFlashReplica = enableTiFlashTest)
      schemaAndDataList.foreach { schemaAndData =>
        startUnsignedTest(schemaAndData, typeName)
      }
    }
  }

  generateTestCases()
}
