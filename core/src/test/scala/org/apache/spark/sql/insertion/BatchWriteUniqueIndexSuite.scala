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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.insertion

import com.pingcap.tispark.datasource.BaseBatchWriteTest
import com.pingcap.tispark.test.generator.DataGenerator._
import com.pingcap.tispark.test.generator.DataType.ReflectedDataType
import com.pingcap.tispark.test.generator.{
  DefaultColumn,
  Index,
  IndexColumn,
  NullableType,
  PrefixColumn,
  Schema,
  SchemaAndData,
  UniqueKey
}
import com.pingcap.tispark.utils.TiUtil
import org.apache.commons.math3.util.Combinations
import org.apache.spark.sql.types.BaseRandomDataTypeTest

import scala.util.Random

class BatchWriteUniqueIndexSuite
    extends BaseBatchWriteTest("batch_write_insertion_one_unique_index", "batch_write_test_index")
    with BaseRandomDataTypeTest {
  override protected def rowCount: Int = 0

  private val writeRowCount = 50

  private val testDesc = "Test for single and multiple unique index type in batch-write insertion"

  // TODO: support binary insertion.
  private val dataTypes: List[ReflectedDataType] =
    integers ::: decimals ::: doubles ::: charCharset

  override protected def genIndex(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      r: Random): List[List[Index]] = {
    val size = dataTypesWithDesc.length
    // the first step is generate all possible keys
    val keyList = scala.collection.mutable.ListBuffer.empty[List[UniqueKey]]
    for (i <- 1 until 3) {
      val combination = new Combinations(size, i)
      //(i, size)
      val iterator = combination.iterator()
      while (iterator.hasNext) {
        val intArray = iterator.next()
        val indexColumnList = scala.collection.mutable.ListBuffer.empty[IndexColumn]
        // index may have multiple column
        for (j <- 0 until intArray.length) {
          // we add extra one to the column id since 1 is reserved to primary key
          if (isStringType(dataTypesWithDesc(intArray(j))._1)) {
            indexColumnList += PrefixColumn(intArray(j) + 1, r.nextInt(4) + 2)
          } else {
            indexColumnList += DefaultColumn(intArray(j) + 1)
          }
        }

        keyList += UniqueKey(indexColumnList.toList) :: Nil
      }
    }

    keyList.toList
  }

  private def startTest(schemaAndDataList: List[SchemaAndData]): Unit = {
    test(s"test unique indices cases $testDesc") {
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
    val dataTypesWithDesc: List[(ReflectedDataType, String, String)] = dataTypes.map {
      genDescription(_, NullableType.NotNullable)
    }

    val schemaAndDataList = genSchemaAndData(
      rowCount,
      dataTypesWithDesc,
      database,
      hasTiFlashReplica = enableTiFlashTest)
    startTest(schemaAndDataList)
  }

  generateTestCases()
}
