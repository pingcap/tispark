package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class ExceptionTestSuite extends BaseDataSourceTest {

  test("Test write to table does not exist") {
    val table = "write_to_table_does_not_exist"
    val row1 = Row(null, "Hello")
    val row2 = Row(2L, "TiDB")

    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("s", StringType)
      )
    )

    dropTable(table)

    val errMsg = s"table `$database`.`$table` does not exists!"
    val caught = intercept[TiBatchWriteException] {
      tidbWriteWithTable(List(row1, row2), schema, table)
    }
    assert(caught.getMessage.equals(errMsg))
  }

  test("Test column does not exist") {
    val table = "col_not_exist"
    val row1 = Row(2L, 3L)

    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("i2", LongType)
      )
    )

    dropTable(table)

    createTable(s"create table `%s`.`%s`(i int)", table)

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWriteWithTable(List(row1), schema, table)
      }
      assert(
        caught.getMessage
          .equals(
            "table without auto increment column, but data col size 2 != table column size 1"
          )
      )
    }
  }

  test("Missing insert column") {
    val table = "missing_insert_column"
    val row1 = Row(2L, 3L)

    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("i2", LongType)
      )
    )

    dropTable(table)

    createTable("create table `%s`.`%s`(i int, i2 int, i3 int)", table)

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWriteWithTable(List(row1), schema, table)
      }
      assert(
        caught.getMessage
          .equals(
            "table without auto increment column, but data col size 2 != table column size 3"
          )
      )
    }
  }

  test("Insert null value to Not Null Column") {
    val table = "insert_null_to_not_null_col"
    val row1 = Row(null, 3L)
    val row2 = Row(4L, null)

    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("i2", LongType)
      )
    )

    dropTable(table)

    createTable("create table `%s`.`%s`(i int, i2 int NOT NULL)", table)

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWriteWithTable(List(row1, row2), schema, table)
      }
      assert(
        caught.getMessage
          .equals(
            "Insert null value to not null column! 1 rows contain illegal null values!"
          )
      )
    }
  }
}
