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

import com.pingcap.tispark.TiConfigConst
import com.pingcap.tispark.datasource.BaseBatchWriteTest
import com.pingcap.tispark.test.generator.DataGenerator._
import com.pingcap.tispark.test.generator.DataType.{ReflectedDataType, VARCHAR}
import com.pingcap.tispark.test.generator._
import com.pingcap.tispark.utils.TiUtil
import org.apache.commons.math3.util.Combinations
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BaseRandomDataTypeTest

import scala.util.Random

class BatchWritePKAndIndexSuite
    extends BaseBatchWriteTest(
      "batch_write_insertion_pk_and_index",
      "batch_write_test_pk_and_index")
    with BaseRandomDataTypeTest {
  override protected def rowCount: Int = 0

  private val writeRowCount = 50

  // TODO: support binary insertion.
  // TODO: support texts
  private val dataTypes: List[ReflectedDataType] =
    integers ::: decimals ::: doubles ::: strings

  // TODO: tidb-4.0 does not support clustered index, should skip test
  private val clusteredIndex: List[Boolean] = true :: false :: Nil

  override protected def genIndex(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      r: Random): List[List[Index]] = {
    val size = dataTypesWithDesc.length
    // the first step is generate all possible keys
    val keyList = scala.collection.mutable.ListBuffer.empty[List[Index]]
    for (i <- 1 until 3) {
      val combination = new Combinations(size - 1, i)
      //(i, size - 1)
      val iterator = combination.iterator()
      while (iterator.hasNext) {
        val intArray = iterator.next()
        val indexColumnList = scala.collection.mutable.ListBuffer.empty[IndexColumn]
        // index may have multiple column
        for (j <- 0 until intArray.length) {
          // we add extra one to the column id since 1 is reserved to primary key
          if (isStringType(dataTypesWithDesc(intArray(j))._1)) {
            // TODO: enalbe test prefix index
            //  data duplicate cannot check answer
            indexColumnList += DefaultColumn(intArray(j) + 1)
            //indexColumnList += PrefixColumn(intArray(j) + 1, r.nextInt(4) + 2)
          } else {
            indexColumnList += DefaultColumn(intArray(j) + 1)
          }
        }

        val primaryKey = PrimaryKey(genIndex(size, dataTypesWithDesc(size - 1)._1) :: Nil)
        keyList += primaryKey :: UniqueKey(indexColumnList.toList) :: Nil
        keyList += primaryKey :: Key(indexColumnList.toList) :: Nil
      }
    }

    keyList.toList
  }

  private def genIndex(i: Int, dataType: ReflectedDataType): IndexColumn = {
    if (isStringType(dataType)) {
      // TODO: enable test prefix index
      // data duplicate cannot check answer
      DefaultColumn(i)
      //PrefixColumn(i, r.nextInt(4) + 2)
    } else {
      DefaultColumn(i)
    }
  }

  test("test pk and unique indices cases") {
    val dataTypesWithDesc: List[(ReflectedDataType, String, String)] =
      (dataTypes ::: VARCHAR :: Nil).map {
        genDescription(_, NullableType.Nullable)
      }

    clusteredIndex.foreach { clusteredIndex =>
      val schemaAndDataList = genSchemaAndData(
        rowCount,
        dataTypesWithDesc,
        database,
        isClusteredIndex = clusteredIndex,
        hasTiFlashReplica = enableTiFlashTest)
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
    checkAnswer(rows, schema.tableName)

    // replace
    val replaceData = genReplaceData(data, schema)
    val replaceRows = replaceData.map(tiRowToSparkRow(_, tiColInfos))
    tidbWriteWithTable(replaceRows, tableSchema, schema.tableName, Some(Map("replace" -> "true")))
    // check data and index consistence
    adminCheck(schema)
    checkAnswer(replaceRows, schema.tableName)
  }

  private def checkAnswer(expectedAnswer: Seq[Row], tableName: String): Unit = {
    spark.sqlContext.setConf(TiConfigConst.USE_INDEX_SCAN_FIRST, "false")
    spark.sql(s"explain select * from `$databaseWithPrefix`.`$tableName`").show(false)
    spark.sql(s"select * from `$databaseWithPrefix`.`$tableName`").show(false)
    checkAnswer(spark.sql(s"select * from `$databaseWithPrefix`.`$tableName`"), expectedAnswer)

    spark.sqlContext.setConf(TiConfigConst.USE_INDEX_SCAN_FIRST, "true")
    spark.sql(s"explain select * from `$databaseWithPrefix`.`$tableName`").show(false)
    checkAnswer(spark.sql(s"select * from `$databaseWithPrefix`.`$tableName`"), expectedAnswer)
    spark.sqlContext.setConf(TiConfigConst.USE_INDEX_SCAN_FIRST, "false")
  }
}
