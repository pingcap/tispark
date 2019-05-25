package com.pingcap.tispark.convert

import java.sql.BatchUpdateException

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * REAL type include:
 * 1. FLOAT
 * 2. DOUBLE
 */
class ToRealSuite extends BaseDataSourceTest("test_data_type_convert_to_real") {

  //  + 1.0E-40f because of this issue: https://github.com/pingcap/tidb/issues/10587
  private val minFloat: java.lang.Float = java.lang.Float.MIN_VALUE + 1.0E-40f
  private val maxFloat: java.lang.Float = java.lang.Float.MAX_VALUE
  private val minDouble: java.lang.Double = java.lang.Double.MIN_VALUE
  private val maxDouble: java.lang.Double = java.lang.Double.MAX_VALUE

  test("Test Convert from java.lang.Boolean to REAL") {
    // success
    // java.lang.Boolean -> FLOAT
    // java.lang.Boolean -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, true, false)
        val row3 = Row(3, false, true)
        val row4 = Row(4, true, true)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", BooleanType),
            StructField("c2", BooleanType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        // insert row1 row2 row3 row4
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4), schema)
    }
  }

  test("Test Convert from java.lang.Byte to REAL") {
    // success
    // java.lang.Byte -> FLOAT
    // java.lang.Byte -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Byte = java.lang.Byte.valueOf("11")
        val b: java.lang.Byte = java.lang.Byte.valueOf("22")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)
        val row3 = Row(3, java.lang.Byte.MAX_VALUE, java.lang.Byte.MIN_VALUE)
        val row4 = Row(4, java.lang.Byte.MIN_VALUE, java.lang.Byte.MAX_VALUE)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ByteType),
            StructField("c2", ByteType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        // insert row1 row2 row3 row4
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4), schema)
    }
  }

  test("Test Convert from java.lang.Short to REAL") {
    // success
    // java.lang.Short -> FLOAT
    // java.lang.Short -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Short = java.lang.Short.valueOf("11")
        val b: java.lang.Short = java.lang.Short.valueOf("22")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)
        val row3 = Row(3, java.lang.Short.MAX_VALUE, java.lang.Short.MIN_VALUE)
        val row4 = Row(4, java.lang.Short.MIN_VALUE, java.lang.Short.MAX_VALUE)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ShortType),
            StructField("c2", ShortType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        // insert row1 row2 row3 row4
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4), schema)
    }
  }

  test("Test Convert from java.lang.Integer to REAL") {
    // success
    // java.lang.Integer -> FLOAT
    // java.lang.Integer -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, 22, 33)
        val row3 = Row(3, java.lang.Integer.MAX_VALUE, java.lang.Integer.MIN_VALUE)
        val row4 = Row(4, java.lang.Integer.MIN_VALUE, java.lang.Integer.MAX_VALUE)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", IntegerType),
            StructField("c2", IntegerType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        // insert row1 row2 row3 row4
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4), schema)
    }
  }

  test("Test Convert from java.lang.Long to REAL") {
    // success
    // java.lang.Long -> FLOAT
    // java.lang.Long -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, 22L, 33L)
        val row3 = Row(3, java.lang.Long.MAX_VALUE, java.lang.Long.MIN_VALUE)
        val row4 = Row(4, java.lang.Long.MIN_VALUE, java.lang.Long.MAX_VALUE)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        // insert row1 row2 row3 row4
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4), schema)
    }
  }

  test("Test Convert from java.lang.Float to REAL") {
    // success
    // java.lang.Float -> FLOAT
    // java.lang.Float -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, 22.2f, 33.3f)
        val row3 = Row(3, maxFloat, maxFloat)
        val row4 = Row(4, minFloat, minFloat)
        val row5 = Row(5, -maxFloat, -maxFloat)
        val row6 = Row(6, -minFloat, -minFloat)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", FloatType),
            StructField("c2", FloatType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Double to REAL") {
    // success
    // java.lang.Double -> FLOAT
    // java.lang.Double -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        //  + 1.0E-40f because of this issue: https://github.com/pingcap/tidb/issues/10587
        val row1 = Row(1, null, null)
        val row2 = Row(2, 22.22d, 33.33d)
        val row3 = Row(3, maxFloat.toDouble, maxDouble)
        val row4 = Row(4, minFloat.toDouble, minDouble)
        val row5 = Row(5, -maxFloat.toDouble, -maxDouble)
        val row6 = Row(6, -minFloat.toDouble, -minDouble)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DoubleType),
            StructField("c2", DoubleType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  // TODO: com.pingcap.tikv.exception.TiBatchWriteException: data -3.4028235E38 < lowerBound -3.4028235E38
  ignore("Test Convert from String to REAL") {
    // success
    // String -> FLOAT
    // String -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, "2.2", "-3.3")
        val row3 = Row(3, minFloat.toString, minDouble.toString)
        val row4 = Row(4, maxFloat.toString, maxDouble.toString)
        val row5 = Row(5, (-minFloat).toString, (-minDouble).toString)
        val row6 = Row(6, (-maxFloat + 1.0E20).toString, (-maxDouble).toString)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.sql.Date to REAL") {
    // failure
    // java.sql.Date -> FLOAT
    // java.sql.Date -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, funcName) =>
        val a: java.sql.Date = new java.sql.Date(0)
        val row1 = Row(1, null, null)
        val row2 = Row.apply(2, a, a)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DateType),
            StructField("c2", DateType)
          )
        )

        dropTable()
        jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")

        val caught = intercept[SparkException] {
          // insert row1 row2
          writeFunc(List(row1, row2), schema, None)
        }
        if ("jdbcWrite".equals(funcName)) {
          assert(caught.getCause.getClass == classOf[BatchUpdateException])
          assert(caught.getCause.getMessage.startsWith("Incorrect float value"))
        } else if ("tidbWrite".equals(funcName)) {
          assert(caught.getCause.getClass == classOf[TiBatchWriteException])
          assert(
            caught.getCause.getMessage.equals(
              "do not support converting from class java.sql.Date to column type: RealType:TypeFloat"
            )
          )
        }
    }
  }

  // TODO: test following types
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
