package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class ErrorSuite extends BaseDataSourceSuite("test_datasource_error") {

  override def beforeAll(): Unit =
    super.beforeAll()

  test("Test write to table does not exist") {
    val row1 = Row(null, "Hello")
    val row2 = Row(2L, "TiDB")

    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("s", StringType)
      )
    )

    dropTable()

    val caught = intercept[TiBatchWriteException] {
      batchWrite(List(row1, row2), schema)
    }
    assert(
      caught.getMessage
        .equals(
          "table tispark_test.test_datasource_error does not exists!"
        )
    )
  }

  test("Test column does not exist") {
    val row1 = Row(2L, 3L)

    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("i2", LongType)
      )
    )

    dropTable()

    jdbcUpdate(s"create table $dbtableInJDBC(i int)")

    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row1), schema)
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
    val row1 = Row(2L, 3L)

    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("i2", LongType)
      )
    )

    dropTable()

    jdbcUpdate(s"create table $dbtableInJDBC(i int, i2 int, i3 int)")

    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row1), schema)
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
    val row1 = Row(null, 3L)
    val row2 = Row(4L, null)

    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("i2", LongType)
      )
    )

    dropTable()

    jdbcUpdate(s"create table $dbtableInJDBC(i int, i2 int NOT NULL)")

    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row1, row2), schema)
      }
      println(caught.getMessage)
      assert(
        caught.getMessage
          .equals(
            "Insert null value to not null column! 1 rows contain illegal null values!"
          )
      )
    }
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
