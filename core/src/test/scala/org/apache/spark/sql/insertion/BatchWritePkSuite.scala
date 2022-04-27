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
  // TODO: support decimals insertion.
  private val dataTypes: List[ReflectedDataType] = integers ::: doubles ::: charCharset

  private val clusteredIndex: List[Boolean] = true :: false :: Nil

  private val testDesc = "Test for PK (one column & two columns) in batch-write insertion"

  override protected def genIndex(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      r: Random): List[List[Index]] = {
    val keyList = scala.collection.mutable.ListBuffer.empty[List[PrimaryKey]]
    val pkColList = dataTypesWithDesc.zipWithIndex.map {
      case (dataType, i) =>
        genIndex(i + 1, dataType._1)
    }
    if (pkColList.nonEmpty) {
      keyList += List(PrimaryKey(pkColList(0) :: Nil))
    }
    if (pkColList.size >= 2) {
      keyList += List(PrimaryKey(pkColList(0) :: pkColList(1) :: Nil))
    }
    keyList.toList
  }

  private def genIndex(i: Int, dataType: ReflectedDataType): IndexColumn = {
    if (isStringType(dataType)) {
      PrefixColumn(i, r.nextInt(4) + 2)
    } else {
      DefaultColumn(i)
    }
  }

  private def startTest(
      schemaAndDataList: List[SchemaAndData],
      typeName: String,
      clusteredIndex: Boolean): Unit = {
    test(s"Test $typeName ClusteredIndex $clusteredIndex - $testDesc") {
      schemaAndDataList.foreach { schemaAndData =>
        loadToDB(schemaAndData)
        setCurrentDatabase(database)
        insertAndReplace(schemaAndData.schema)
      }
    }
  }

  private def insertAndReplace(schema: Schema): Unit = {
    val tiTblInfo = getTableInfo(schema.database, schema.tableName)
    val tiColInfos = tiTblInfo.getColumns
    val tableSchema = TiUtil.getSchemaFromTable(tiTblInfo)
    val data = generateRandomRows(schema, writeRowCount, r)

    // check tiflash ready
    if (enableTiFlashTest) {
      if (!checkLoadTiFlashWithRetry(schema.tableName, Some(schema.database))) {
        log.warn("TiFlash is not ready")
        cancel()
      }
    }

    // gen data
    val rows = data.map(tiRowToSparkRow(_, tiColInfos))
    // insert data to tikv
    tidbWriteWithTable(rows, tableSchema, schema.tableName)
    // check data and index consistence
    adminCheck(schema)
    checkAnswer(spark.sql(s"select * from `$databaseWithPrefix`.`${schema.tableName}`"), rows)

    // replace
    val replaceData = genReplaceData(data, schema)
    val replaceRows = replaceData.map(tiRowToSparkRow(_, tiColInfos))
    tidbWriteWithTable(replaceRows, tableSchema, schema.tableName, Some(Map("replace" -> "true")))
    // check data and index consistence
    adminCheck(schema)
    checkAnswer(
      spark.sql(s"select * from `$databaseWithPrefix`.`${schema.tableName}`"),
      replaceRows)

  }

  private def generateTestCases(): Unit = {
    for (pk <- dataTypes) {
      val dataTypes = List(pk, INT)
      val dataTypesWithDesc: List[(ReflectedDataType, String, String)] = dataTypes.map {
        genDescription(_, NullableType.Nullable)
      }

      clusteredIndex.foreach { clusteredIndex =>
        val schemaAndDataList = genSchemaAndData(
          rowCount,
          dataTypesWithDesc,
          database,
          isClusteredIndex = clusteredIndex,
          hasTiFlashReplica = enableTiFlashTest)
        startTest(schemaAndDataList, getTypeName(pk), clusteredIndex)
      }
    }
  }

  generateTestCases()
}
