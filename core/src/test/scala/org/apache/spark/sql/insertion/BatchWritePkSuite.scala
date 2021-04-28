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

package org.apache.spark.sql.insertion

import com.pingcap.tispark.datasource.BaseBatchWriteTest
import com.pingcap.tispark.test.generator.DataGenerator._
import com.pingcap.tispark.test.generator.DataType._
import com.pingcap.tispark.test.generator._
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.types.BaseRandomDataTypeTest

import scala.util.Random

class BatchWritePkSuite
    extends BaseBatchWriteTest("batch_write_insertion_pk", "batch_write_test_pk")
    with BaseRandomDataTypeTest {
  override protected def rowCount: Int = 0

  private val writeRowCount = 50

  // TODO: support binary insertion.
  private val dataTypes: List[ReflectedDataType] =
    integers ::: decimals ::: doubles ::: charCharset

  private val testDesc = "Test for single PK column in batch-write insertion"

  override protected def genIndex(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      r: Random): List[List[Index]] = {
    val size = dataTypesWithDesc.length
    val keyList = scala.collection.mutable.ListBuffer.empty[List[PrimaryKey]]
    for (i <- 0 until size) {
      // we add extra one to the column id since 1 is reserved to primary key
      val pkCol = if (isStringType(dataTypesWithDesc(i)._1)) {
        PrefixColumn(i + 1, r.nextInt(4) + 2) :: Nil
      } else {
        DefaultColumn(i + 1) :: Nil
      }
      keyList += PrimaryKey(pkCol) :: Nil
    }
    keyList.toList
  }

  private def startTest(schemaAndDataList: List[SchemaAndData], typeName: String): Unit = {
    test(s"Test $typeName - $testDesc") {
      schemaAndDataList.foreach { schemaAndData =>
        loadToDB(schemaAndData)
        setCurrentDatabase(database)
        insertAndSelect(schemaAndData.schema)
      }
    }
  }

  private def insertAndSelect(schema: Schema): Unit = {
    val tiTblInfo = getTableInfo(schema.database, schema.tableName)
    val tiColInfos = tiTblInfo.getColumns
    // gen data
    val rows =
      generateRandomRows(schema, writeRowCount, r).map { row =>
        tiRowToSparkRow(row, tiColInfos)
      }
    // insert data to tikv
    tidbWriteWithTable(rows, TiUtil.getSchemaFromTable(tiTblInfo), schema.tableName)
    // select data from tikv and compare with tidb
    compareTiDBSelectWithJDBCWithTable_V2(tblName = schema.tableName, schema.pkColumnName)
  }

  private def generateTestCases(): Unit = {
    for (pk <- dataTypes) {
      val dataTypes = List(pk, INT)
      val dataTypesWithDesc: List[(ReflectedDataType, String, String)] = dataTypes.map {
        genDescription(_, NullableType.NumericNotNullable)
      }

      val schemaAndDataList = genSchemaAndData(
        rowCount,
        dataTypesWithDesc,
        database,
        hasTiFlashReplica = enableTiFlashTest)
      startTest(schemaAndDataList, getTypeName(pk))
    }
  }

  generateTestCases()
}
