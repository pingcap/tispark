package com.pingcap.tispark.convert

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * DATE type include:
 * 1. DATE
 */
class ToDateSuite extends BaseDataSourceTest {

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

  val tableTemplate: String = "test_%s_to_date"
  val queryTemplate: String = "create table `%s`.`%s`(i INT, c1 DATE, c2 DATE)"

  override def beforeAll(): Unit = {
    super.beforeAll()

    val readA: java.sql.Date = java.sql.Date.valueOf("2019-11-11")
    val readB: java.sql.Date = java.sql.Date.valueOf("1989-12-30")

    readRow1 = Row(1, null, null)
    readRow2 = Row(2, readA, readB)
  }

  test("Test Convert from java.lang.Long to DATE") {
    // success
    // java.lang.Long -> DATE
    val table = tableTemplate.format("long")
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

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema, table)
      case (writeFunc, "jdbcWrite") =>
      //ignored, because of this error
      //java.sql.BatchUpdateException: Data truncation: invalid time format: '34'
    }
  }

  test("Test Convert from String to DATE") {
    // success
    // String -> DATE
    val table = tableTemplate.format("str")
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

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema, table)
    }
  }

  test("Test Convert from java.sql.Date to DATE") {
    // success
    // java.sql.Date -> DATE
    val table = tableTemplate.format("date")
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

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2), schema, table)
    }
  }

  test("Test Convert from java.sql.Timestamp to DATE") {
    // success
    // java.sql.Timestamp -> DATE
    val table = tableTemplate.format("long")
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

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema, table)
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
}
