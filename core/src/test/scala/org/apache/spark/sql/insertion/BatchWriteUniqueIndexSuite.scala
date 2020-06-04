package org.apache.spark.sql.insertion

import com.pingcap.tispark.datasource.BaseDataSourceTest
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.test.generator.DataType.ReflectedDataType
import org.apache.spark.sql.test.generator.Schema
import org.apache.spark.sql.test.generator.TestDataGenerator._

class BatchWriteUniqueIndexSuite
    extends BaseDataSourceTest("batch_write_insertion_one_unique_index", "batch_write_test_index")
    with EnumerateUniqueIndexDataTypeTestAction {
  // TODO: support binary insertion.
  override val dataTypes: List[ReflectedDataType] = integers ::: decimals ::: doubles ::: charCharset
  override val unsignedDataTypes: List[ReflectedDataType] = integers ::: decimals ::: doubles
  override val database = "batch_write_test_index"
  override val testDesc = "Test for single and multiple unique index type in batch-write insertion"

  override def beforeAll(): Unit = {
    super.beforeAll()
    tidbStmt.execute(s"drop database if exists $database")
    tidbStmt.execute(s"create database $database")
  }

  private def dropAndCreateTbl(schema: Schema): Unit = {
    // drop table if exits
    dropTable(schema.tableName)

    // create table in tidb first
    jdbcUpdate(schema.toString)
  }

  private def insertAndSelect(schema: Schema): Unit = {
    val tblName = schema.tableName

    val tiTblInfo = getTableInfo(database, tblName)
    val tiColInfos = tiTblInfo.getColumns
    // gen data
    val rows =
      generateRandomRows(schema, rowCount, r).map(row => tiRowToSparkRow(row, tiColInfos))
    // insert data to tikv
    tidbWriteWithTable(rows, TiUtil.getSchemaFromTable(tiTblInfo), tblName)
    // select data from tikv and compare with tidb
    compareTiDBSelectWithJDBCWithTable_V2(tblName = tblName, "col_bigint")
  }

  test("test unique indices cases") {
    val schemas = genSchema(dataTypes, table)

    schemas.foreach { schema =>
      dropAndCreateTbl(schema)
    }

    schemas.foreach { schema =>
      insertAndSelect(schema)
    }
  }

  // this is only for mute the warning
  override def test(): Unit = {}

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
