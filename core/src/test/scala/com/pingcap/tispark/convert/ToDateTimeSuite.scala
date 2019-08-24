package com.pingcap.tispark.convert

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * DATETIME type include:
 * 1. DATETIME
 */
class ToDateTimeSuite extends BaseDataSourceTest {

  private var readRow1: Row = _
  private var readRow2: Row = _

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", TimestampType),
      StructField("c2", TimestampType),
      StructField("c2", TimestampType)
    )
  )

  val queryTemplate: String =
    "create table `%s`.`%s`(i INT, c1 DATETIME(0), c2 DATETIME(3), c3 DATETIME(6))"
  val tableTemplate: String = "test_%s_to_datetime"

  override def beforeAll(): Unit = {
    super.beforeAll()

    val readA: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 11:11:11")
    val readB: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999")
    val readC: java.sql.Timestamp = java.sql.Timestamp.valueOf("1995-05-01 21:21:21.666666")

    readRow1 = Row(1, null, null, null)
    readRow2 = Row(2, readA, readB, readC)
  }

  test("Test Convert from String to DATETIME") {
    // success
    // String -> DATETIME
    val table = tableTemplate.format("str")
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null, null)
        val row2 =
          Row(2, "2019-11-11 11:11:11", "1990-01-01 01:01:01.999", "1995-05-01 21:21:21.666666")

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType),
            StructField("c3", StringType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema, table)
    }
  }

  test("Test Convert from java.sql.Date to DATETIME") {
    // success
    // java.sql.Date -> DATETIME
    val table = tableTemplate.format("date")
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.sql.Date = java.sql.Date.valueOf("2019-11-11")
        val b: java.sql.Date = java.sql.Date.valueOf("1990-01-01")
        val c: java.sql.Date = java.sql.Date.valueOf("1995-05-01")

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, a, b, c)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DateType),
            StructField("c2", DateType),
            StructField("c3", DateType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2), schema, table)
    }
  }

  test("Test Convert from java.sql.Timestamp to DATETIME") {
    // success
    // java.sql.Timestamp -> DATETIME
    val table = tableTemplate.format("ts")
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 11:11:11")
        val b: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999")
        val c: java.sql.Timestamp = java.sql.Timestamp.valueOf("1995-05-01 21:21:21.666666")

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, a, b, c)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", TimestampType),
            StructField("c2", TimestampType),
            StructField("c3", TimestampType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2), schema, table)
    }
  }

  // mysql jdbc do not support following conversion
  // java.lang.Long

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
