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
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.Schema
import org.apache.spark.sql.test.generator.TestDataGenerator._

class BatchWritePkSuite
    extends BaseBatchWriteTest("batch_write_insertion_pk", "batch_write_test_pk")
    with EnumeratePKDataTypeTestAction {
  // TODO: support binary insertion.
  override def dataTypes: List[ReflectedDataType] =
    integers ::: decimals ::: doubles ::: charCharset
  override def unsignedDataTypes: List[ReflectedDataType] = integers ::: decimals ::: doubles
  override val dbName: String = database
  override val testDesc = "Test for single PK column in batch-write insertion"

  override def beforeAll(): Unit = {
    super.beforeAll()
    tidbStmt.execute(s"drop database if exists $dbName")
    tidbStmt.execute(s"create database $dbName")
  }

  // this is only for mute the warning
  override def test(): Unit = {}

  test("test pk cases") {
    for (pk <- dataTypes) {
      val schemas = genSchema(List(pk, INT), table)

      schemas.foreach { schema =>
        dropAndCreateTbl(schema)
      }

      schemas.foreach { schema =>
        insertAndSelect(schema)
      }
    }
  }

  test("unsigned pk cases") {
    for (pk <- unsignedDataTypes) {
      val schemas = genSchema(List(pk, INT), table)

      schemas.foreach { schema =>
        dropAndCreateTbl(schema)
      }

      schemas.foreach { schema =>
        insertAndSelect(schema)
      }
    }
  }

  private def dropAndCreateTbl(schema: Schema): Unit = {
    // drop table if exits
    dropTable(schema.tableName)

    // create table in tidb first
    jdbcUpdate(schema.toString)
  }

  private def insertAndSelect(schema: Schema): Unit = {
    val tblName = schema.tableName

    val tiTblInfo = getTableInfo(dbName, tblName)
    val tiColInfos = tiTblInfo.getColumns
    // gen data
    val rows =
      generateRandomRows(schema, rowCount, r).map(row => tiRowToSparkRow(row, tiColInfos))
    // insert data to tikv
    tidbWriteWithTable(rows, TiUtil.getSchemaFromTable(tiTblInfo), tblName)
    // select data from tikv and compare with tidb
    compareTiDBSelectWithJDBCWithTable_V2(tblName = tblName, schema.pkColumnName)
  }
}
