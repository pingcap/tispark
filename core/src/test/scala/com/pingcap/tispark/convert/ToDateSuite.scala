package com.pingcap.tispark.convert

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * DATE type include:
 * 1. DATE
 */
class ToDateSuite extends BaseDataSourceTest("test_data_type_convert_to_date") {

  private var readRow1: Row = _
  private var readRow2: Row = _

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", DateType),
      StructField("c2", DateType),
      StructField("c3", DateType)
    )
  )

  private def createTable(): Unit =
    jdbcUpdate(s"create table $dbtable(i INT, c1 DATE, c2 DATE)")

  override def beforeAll(): Unit = {
    super.beforeAll()

    val readA: java.sql.Date = java.sql.Date.valueOf("2019-11-11")
    val readB: java.sql.Date = java.sql.Date.valueOf("1989-12-30")

    readRow1 = Row(1, null, null)
    readRow2 = Row(2, readA, readB)
  }

  test("Test Convert from java.lang.Long to DATE") {
    if (!supportBatchWrite) {
      cancel
    }

    // success
    // java.lang.Long -> DATE
    compareTiDBWriteWithJDBC {
      case (writeFunc, "tidbWrite") =>
        val a: java.lang.Long = java.sql.Date.valueOf("2019-11-11").getTime
        val b: java.lang.Long = java.sql.Date.valueOf("1989-12-30").getTime

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema)
      case (writeFunc, "jdbcWrite") =>
      //ignored, because of this error
      //java.sql.BatchUpdateException: Data truncation: invalid time format: '34'
    }
  }

  test("Test Convert from String to DATE") {
    if (!supportBatchWrite) {
      cancel
    }

    // success
    // String -> DATE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.String = "2019-11-11"
        val b: java.lang.String = "1989-12-30"

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema)
    }
  }

  test("Test Convert from java.sql.Date to DATE") {
    if (!supportBatchWrite) {
      cancel
    }

    // success
    // java.sql.Date -> DATE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.sql.Date = java.sql.Date.valueOf("2019-11-11")
        val b: java.sql.Date = java.sql.Date.valueOf("1989-12-30")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DateType),
            StructField("c2", DateType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2), schema)
    }
  }

  test("Test Convert from java.sql.Timestamp to DATE") {
    if (!supportBatchWrite) {
      cancel
    }

    // success
    // java.sql.Timestamp -> DATE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 11:11:11")
        val b: java.sql.Timestamp = java.sql.Timestamp.valueOf("1989-12-30 01:01:01.999999")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", TimestampType),
            StructField("c2", TimestampType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema)
    }
  }

  // TODO: test following types
  // java.math.BigDecimal
  // java.lang.Boolean
  // java.lang.Byte
  // java.lang.Short
  // java.lang.Integer
  // java.lang.Float
  // java.lang.Double
  // Array[String]
  // scala.collection.Seq
  // scala.collection.Map
  // org.apache.spark.sql.Row

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
