package com.pingcap.tispark.convert

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * TIMESTAMP type include:
 * 1. TIMESTAMP
 */
class ToTimestampSuite extends BaseDataSourceTest("test_data_type_convert_to_timestamp") {

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", TimestampType),
      StructField("c2", TimestampType)
    )
  )

  private def createTable(): Unit =
    jdbcUpdate(s"create table $dbtable(i INT, c1 TIMESTAMP, c2 TIMESTAMP(6))")

  test("Test Convert from java.lang.Long to TIMESTAMP") {
    // success
    // java.lang.Long -> TIMESTAMP
    compareTiDBWriteWithJDBC {
      case (writeFunc, "tidbWrite") =>
        val a: java.lang.Long = java.sql.Timestamp.valueOf("2019-11-11 11:11:11").getTime
        val b: java.lang.Long = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999").getTime

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType)
          )
        )

        val readA: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 11:11:11")
        val readB: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999")

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, readA, readB)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema)
      case (writeFunc, "jdbcWrite") =>
      // TODO: ignored, because of this error
      //java.sql.BatchUpdateException: Data truncation: invalid time format: '34'
    }
  }

  // TODO: to support
  ignore("Test Convert from String to TIMESTAMP") {
    // success
    // String -> TIMESTAMP
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, "2019-11-11 11:11:11", "1990-01-01 01:01:01.999")

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType)
          )
        )

        val readA: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 04:11:11")
        val readB: java.sql.Timestamp = java.sql.Timestamp.valueOf("1989-12-31 18:01:01.999")

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, readA, readB)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema)
    }
  }

  // TODO: to support
  ignore("Test Convert from java.sql.Date to TIMESTAMP") {
    // success
    // java.sql.Date -> TIMESTAMP
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.sql.Date = java.sql.Date.valueOf("2019-11-11")
        val b: java.sql.Date = java.sql.Date.valueOf("1990-01-01")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DateType),
            StructField("c2", DateType)
          )
        )

        val readA: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-10 17:00:00.0")
        val readB: java.sql.Timestamp = java.sql.Timestamp.valueOf("1989-12-31 17:00:00.0")

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, readA, readB)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2), readSchema)
    }
  }

  // TODO: to support
  ignore("Test Convert from java.sql.Timestamp to TIMESTAMP") {
    // success
    // java.sql.Timestamp -> TIMESTAMP
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 11:11:11")
        val b: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", TimestampType),
            StructField("c2", TimestampType)
          )
        )

        val readA: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 04:11:11")
        val readB: java.sql.Timestamp = java.sql.Timestamp.valueOf("1989-12-31 18:01:01.999")

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, readA, readB)

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
