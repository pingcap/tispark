package com.pingcap.tispark.convert

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

/**
 * BYTES type include:
 * 1. BINARY
 * 2. VARBINARY
 * 3. TINYBLOB
 * 4. BLOB
 * 5. MEDIUMBLOB
 * 6. LONGBLOB
 */
class ToBytesSuite extends BaseDataSourceTest("test_data_type_convert_to_bytes") {

  private def createTable(): Unit =
    jdbcUpdate(
      s"create table $dbtable(i INT, c2 VARBINARY(255), c3 TINYBLOB, c4 BLOB, c5 MEDIUMBLOB, c6 LONGBLOB)"
    )

  test("Test Convert from java.lang.Boolean to BYTES") {
    // success
    // java.lang.Boolean -> BYTES
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Boolean = true
        val b: java.lang.Boolean = false

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, b, a, b, a, b)
        val row3 = Row(3, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", BooleanType),
            StructField("c3", BooleanType),
            StructField("c4", BooleanType),
            StructField("c5", BooleanType),
            StructField("c6", BooleanType)
          )
        )

        val readA: java.lang.Long = 49L
        val readB: java.lang.Long = 48L

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 = Row(2, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readA, readA, readA, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readB, readB)

        val readSchema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", LongType),
            StructField("c3", LongType),
            StructField("c4", LongType),
            StructField("c5", LongType),
            StructField("c6", LongType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        // TODO: skipTiDBAndExpectedAnswerCheck because spark returns WrappedArray Type
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          skipTiDBAndExpectedAnswerCheck = true
        )
    }
  }

  test("Test Convert from java.lang.Byte to BYTES") {
    // success
    // java.lang.Byte -> BYTES
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Byte = java.lang.Byte.valueOf("11")
        val b: java.lang.Byte = java.lang.Byte.valueOf("-22")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, b, a, b, a, b)
        val row3 = Row(3, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", ByteType),
            StructField("c3", ByteType),
            StructField("c4", ByteType),
            StructField("c5", ByteType),
            StructField("c6", ByteType)
          )
        )

        val readA: Array[Byte] = Array(49.toByte, 49.toByte)
        val readB: Array[Byte] = Array(45.toByte, 50.toByte, 50.toByte)

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 = Row(2, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readA, readA, readA, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readB, readB)

        val readSchema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", BinaryType),
            StructField("c3", BinaryType),
            StructField("c4", BinaryType),
            StructField("c5", BinaryType),
            StructField("c6", BinaryType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        // TODO: skipTiDBAndExpectedAnswerCheck because spark returns WrappedArray Type
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          skipTiDBAndExpectedAnswerCheck = true
        )
    }
  }

  test("Test Convert from java.lang.Short to BYTES") {
    // success
    // java.lang.Short -> BYTES
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Short = java.lang.Short.valueOf("11")
        val b: java.lang.Short = java.lang.Short.valueOf("-22")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, b, a, b, a, b)
        val row3 = Row(3, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", ShortType),
            StructField("c3", ShortType),
            StructField("c4", ShortType),
            StructField("c5", ShortType),
            StructField("c6", ShortType)
          )
        )

        val readA: Array[Byte] = Array(49.toByte, 49.toByte)
        val readB: Array[Byte] = Array(45.toByte, 50.toByte, 50.toByte)

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 = Row(2, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readA, readA, readA, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readB, readB)

        val readSchema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", BinaryType),
            StructField("c3", BinaryType),
            StructField("c4", BinaryType),
            StructField("c5", BinaryType),
            StructField("c6", BinaryType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        // TODO: skipTiDBAndExpectedAnswerCheck because spark returns WrappedArray Type
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          skipTiDBAndExpectedAnswerCheck = true
        )
    }
  }

  test("Test Convert from java.lang.Integer to BYTES") {
    // success
    // java.lang.Integer -> BYTES
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Integer = java.lang.Integer.valueOf("11")
        val b: java.lang.Integer = java.lang.Integer.valueOf("-22")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, b, a, b, a, b)
        val row3 = Row(3, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", IntegerType),
            StructField("c3", IntegerType),
            StructField("c4", IntegerType),
            StructField("c5", IntegerType),
            StructField("c6", IntegerType)
          )
        )

        val readA: Array[Byte] = Array(49.toByte, 49.toByte)
        val readB: Array[Byte] = Array(45.toByte, 50.toByte, 50.toByte)

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 = Row(2, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readA, readA, readA, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readB, readB)

        val readSchema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", BinaryType),
            StructField("c3", BinaryType),
            StructField("c4", BinaryType),
            StructField("c5", BinaryType),
            StructField("c6", BinaryType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        // TODO: skipTiDBAndExpectedAnswerCheck because spark returns WrappedArray Type
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          skipTiDBAndExpectedAnswerCheck = true
        )
    }
  }

  test("Test Convert from java.lang.Long to BYTES") {
    // success
    // java.lang.Long -> BYTES
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Long = java.lang.Long.valueOf("11")
        val b: java.lang.Long = java.lang.Long.valueOf("-22")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, b, a, b, a, b)
        val row3 = Row(3, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", LongType),
            StructField("c3", LongType),
            StructField("c4", LongType),
            StructField("c5", LongType),
            StructField("c6", LongType)
          )
        )

        val readA: Array[Byte] = Array(49.toByte, 49.toByte)
        val readB: Array[Byte] = Array(45.toByte, 50.toByte, 50.toByte)

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 = Row(2, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readA, readA, readA, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readB, readB)

        val readSchema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", BinaryType),
            StructField("c3", BinaryType),
            StructField("c4", BinaryType),
            StructField("c5", BinaryType),
            StructField("c6", BinaryType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        // TODO: skipTiDBAndExpectedAnswerCheck because spark returns WrappedArray Type
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          skipTiDBAndExpectedAnswerCheck = true
        )
    }
  }

  // TODO: ignored
  ignore("Test Convert from java.lang.Float to BYTES") {
    // success
    // java.lang.Float -> BYTES
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Float = java.lang.Float.valueOf("1.1")
        val b: java.lang.Float = java.lang.Float.valueOf("-2.2")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, b, a, b, a, b)
        val row3 = Row(3, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", FloatType),
            StructField("c3", FloatType),
            StructField("c4", FloatType),
            StructField("c5", FloatType),
            StructField("c6", FloatType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5), schema)
    }
  }

  // TODO: java.sql.BatchUpdateException: Data truncation: Data too long for column 'c1' at row 1
  ignore("Test Convert from java.lang.Double to BYTES") {
    // success
    // java.lang.Double -> BYTES
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Double = java.lang.Double.valueOf("1.1")
        val b: java.lang.Double = java.lang.Double.valueOf("-2.2")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, b, a, b, a, b)
        val row3 = Row(3, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", DoubleType),
            StructField("c3", DoubleType),
            StructField("c4", DoubleType),
            StructField("c5", DoubleType),
            StructField("c6", DoubleType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5), schema)
    }
  }

  test("Test Convert from String to BYTES") {
    // success
    // java.lang.String -> BYTES
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.String = new java.lang.String("11")
        val b: java.lang.String = new java.lang.String("-22")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, b, a, b, a, b)
        val row3 = Row(3, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", StringType),
            StructField("c3", StringType),
            StructField("c4", StringType),
            StructField("c5", StringType),
            StructField("c6", StringType)
          )
        )

        val readA: Array[Byte] = Array(49.toByte, 49.toByte)
        val readB: Array[Byte] = Array(45.toByte, 50.toByte, 50.toByte)

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 = Row(2, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readA, readA, readA, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readB, readB)

        val readSchema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c2", BinaryType),
            StructField("c3", BinaryType),
            StructField("c4", BinaryType),
            StructField("c5", BinaryType),
            StructField("c6", BinaryType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        // TODO: skipTiDBAndExpectedAnswerCheck because spark returns WrappedArray Type
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          skipTiDBAndExpectedAnswerCheck = true
        )
    }
  }

  // TODO: test following types
  // java.sql.Date
  // java.math.BigDecimal
  // java.sql.Timestamp
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
