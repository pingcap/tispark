package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class CheckUnsupportedSuite extends BaseDataSourceTest {

  override def beforeAll(): Unit =
    super.beforeAll()

  test("Test write to partition table") {
    val table = "check_unsupported_write_to_part_tbl"
    dropTable(table)

    tidbStmt.execute("set @@tidb_enable_table_partition = 1")

    createTable(
      s"create table `%s`.`%s`(i int, s varchar(128)) partition by range(i) (partition p0 values less than maxvalue)",
      table
    )
    jdbcUpdate(
      s"insert into `%s`.`%s` values(null, 'Hello')",
      table
    )

    val row1 = Row(null, "Hello")
    val row2 = Row(2, "TiDB")
    val row3 = Row(3, "Spark")

    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("s", StringType)
      )
    )

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWriteWithTable(List(row2, row3), schema, table)
      }
      assert(
        caught.getMessage
          .equals("tispark currently does not support write data to partition table!")
      )
    }

    testTiDBSelectWithTable(Seq(row1), tableName = table)
  }

  test("Check Virtual Generated Column") {
    val table = "check_unsupported_write_virtual_generated_col"
    dropTable(table)
    createTable(
      s"create table `%s`.`%s`(i INT, c1 INT, c2 INT,  c3 INT AS (c1 + c2))",
      table
    )

    val row1 = Row(1, 2, 3)
    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("c1", IntegerType),
        StructField("c2", IntegerType)
      )
    )

    val caught = intercept[TiBatchWriteException] {
      tidbWriteWithTable(List(row1), schema, table)
    }
    assert(
      caught.getMessage
        .equals("tispark currently does not support write data to table with generated column!")
    )

  }

  test("Check Stored Generated Column") {
    val table = "check_unsupported_write_to_stored_generated_col"
    createTable(
      s"create table `%s`.`%s`(i INT, c1 INT, c2 INT,  c3 INT AS (c1 + c2) STORED)",
      table
    )

    val row1 = Row(1, 2, 3)
    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("c1", IntegerType),
        StructField("c2", IntegerType)
      )
    )
    val caught = intercept[TiBatchWriteException] {
      tidbWriteWithTable(List(row1), schema, table)
    }
    assert(
      caught.getMessage
        .equals("tispark currently does not support write data to table with generated column!")
    )

  }
}
