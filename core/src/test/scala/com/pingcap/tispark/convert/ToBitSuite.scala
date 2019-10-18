package com.pingcap.tispark.convert

import com.pingcap.tikv.types.TypeSystem
import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * BIT type include:
 * 1. BIT
 */
class ToBitSuite extends BaseDataSourceTest("test_data_type_convert_to_bit") {

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", BooleanType),
      StructField("c2", BinaryType),
      StructField("c3", BinaryType)
    )
  )

  private def createTable(): Unit =
    jdbcUpdate(
      s"create table $dbtable(i INT, c1 BIT(1), c2 BIT(8),  c3 BIT(64))"
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    TypeSystem.setVersion(1)
  }

  test("Test Convert from java.lang.Boolean to BIT") {
    // success
    // java.lang.Boolean -> BIT
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null, null)
        val row2 = Row(2, false, true, false)
        val row3 = Row(3, true, false, true)
        val row4 = Row(4, false, false, false)
        val row5 = Row(5, true, true, true)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", BooleanType),
            StructField("c2", BooleanType),
            StructField("c3", BooleanType)
          )
        )

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, false, toByteArray(1), toByteArray(0, 0, 0, 0, 0, 0, 0, 0))
        val readRow3 = Row(3, true, toByteArray(0), toByteArray(0, 0, 0, 0, 0, 0, 0, 1))
        val readRow4 = Row(4, false, toByteArray(0), toByteArray(0, 0, 0, 0, 0, 0, 0, 0))
        val readRow5 = Row(5, true, toByteArray(1), toByteArray(0, 0, 0, 0, 0, 0, 0, 1))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2, readRow3, readRow4, readRow5), readSchema)
    }
  }

  test("Test Convert from java.lang.Byte to BIT") {
    // success
    // java.lang.Byte -> BIT
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val zero: java.lang.Byte = java.lang.Byte.valueOf("0")
        val one: java.lang.Byte = java.lang.Byte.valueOf("1")
        val a: java.lang.Byte = java.lang.Byte.valueOf("11")
        val b: java.lang.Byte = java.lang.Byte.valueOf("22")
        val c: java.lang.Byte = java.lang.Byte.MAX_VALUE
        val d: java.lang.Byte = java.lang.Byte.valueOf("102")

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, zero, b, a)
        val row3 = Row(3, one, a, b)
        val row4 = Row(4, zero, a, c)
        val row5 = Row(5, one, b, d)

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, false, toByteArray(22), toByteArray(0, 0, 0, 0, 0, 0, 0, 11))
        val readRow3 = Row(3, true, toByteArray(11), toByteArray(0, 0, 0, 0, 0, 0, 0, 22))
        val readRow4 = Row(4, false, toByteArray(11), toByteArray(0, 0, 0, 0, 0, 0, 0, 127))
        val readRow5 = Row(5, true, toByteArray(22), toByteArray(0, 0, 0, 0, 0, 0, 0, 102))

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ByteType),
            StructField("c2", ByteType),
            StructField("c3", ByteType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2, readRow3, readRow4, readRow5), readSchema)
    }
  }

  test("Test Convert from java.lang.Short to BIT") {
    // success
    // java.lang.Short -> BIT
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val zero: java.lang.Short = java.lang.Short.valueOf("0")
        val one: java.lang.Short = java.lang.Short.valueOf("1")
        val a: java.lang.Short = java.lang.Short.valueOf("11")
        val b: java.lang.Short = java.lang.Short.valueOf("22")
        val c: java.lang.Short = java.lang.Short.MAX_VALUE
        val d: java.lang.Short = java.lang.Short.valueOf("102")

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, zero, b, a)
        val row3 = Row(3, one, a, b)
        val row4 = Row(4, zero, a, c)
        val row5 = Row(5, one, b, d)

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, false, toByteArray(22), toByteArray(0, 0, 0, 0, 0, 0, 0, 11))
        val readRow3 = Row(3, true, toByteArray(11), toByteArray(0, 0, 0, 0, 0, 0, 0, 22))
        val readRow4 = Row(4, false, toByteArray(11), toByteArray(0, 0, 0, 0, 0, 0, 127, 255))
        val readRow5 = Row(5, true, toByteArray(22), toByteArray(0, 0, 0, 0, 0, 0, 0, 102))

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ShortType),
            StructField("c2", ShortType),
            StructField("c3", ShortType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2, readRow3, readRow4, readRow5), readSchema)
    }
  }

  test("Test Convert from java.lang.Integer to BIT") {
    // success
    // java.lang.Integer -> BIT
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val zero: java.lang.Integer = java.lang.Integer.valueOf("0")
        val one: java.lang.Integer = java.lang.Integer.valueOf("1")
        val a: java.lang.Integer = java.lang.Integer.valueOf("11")
        val b: java.lang.Integer = java.lang.Integer.valueOf("22")
        val c: java.lang.Integer = java.lang.Integer.MAX_VALUE
        val d: java.lang.Integer = java.lang.Integer.valueOf("102")

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, zero, b, a)
        val row3 = Row(3, one, a, b)
        val row4 = Row(4, zero, a, c)
        val row5 = Row(5, one, b, d)

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, false, toByteArray(22), toByteArray(0, 0, 0, 0, 0, 0, 0, 11))
        val readRow3 = Row(3, true, toByteArray(11), toByteArray(0, 0, 0, 0, 0, 0, 0, 22))
        val readRow4 = Row(4, false, toByteArray(11), toByteArray(0, 0, 0, 0, 127, 255, 255, 255))
        val readRow5 = Row(5, true, toByteArray(22), toByteArray(0, 0, 0, 0, 0, 0, 0, 102))

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", IntegerType),
            StructField("c2", IntegerType),
            StructField("c3", IntegerType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2, readRow3, readRow4, readRow5), readSchema)
    }
  }

  test("Test Convert from java.lang.Long to BIT") {
    // success
    // java.lang.Long -> BIT
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val zero: java.lang.Long = java.lang.Long.valueOf("0")
        val one: java.lang.Long = java.lang.Long.valueOf("1")
        val a: java.lang.Long = java.lang.Long.valueOf("11")
        val b: java.lang.Long = java.lang.Long.valueOf("22")
        val c: java.lang.Long = java.lang.Long.MAX_VALUE
        val d: java.lang.Long = java.lang.Long.valueOf("102")

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, zero, b, a)
        val row3 = Row(3, one, a, b)
        val row4 = Row(4, zero, a, c)
        val row5 = Row(5, one, b, d)

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, false, toByteArray(22), toByteArray(0, 0, 0, 0, 0, 0, 0, 11))
        val readRow3 = Row(3, true, toByteArray(11), toByteArray(0, 0, 0, 0, 0, 0, 0, 22))
        val readRow4 =
          Row(4, false, toByteArray(11), toByteArray(127, 255, 255, 255, 255, 255, 255, 255))
        val readRow5 = Row(5, true, toByteArray(22), toByteArray(0, 0, 0, 0, 0, 0, 0, 102))

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType),
            StructField("c3", LongType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2, readRow3, readRow4, readRow5), readSchema)
    }
  }

  test("Test Convert from java.lang.Float to BIT") {
    // success
    // java.lang.Float -> BIT
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val zero: java.lang.Float = 0f
        val one: java.lang.Float = 1f
        val a: java.lang.Float = 1.1f
        val b: java.lang.Float = 2.2f
        val c: java.lang.Float = 3.4e+3f
        val d: java.lang.Float = 1.4e-2f

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, zero, b, c)
        val row3 = Row(3, one, a, d)
        val row4 = Row(4, zero, a, a)
        val row5 = Row(5, one, b, b)

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, false, toByteArray(2), toByteArray(0, 0, 0, 0, 0, 0, 13, 72))
        val readRow3 = Row(3, true, toByteArray(1), toByteArray(0, 0, 0, 0, 0, 0, 0, 0))
        val readRow4 = Row(4, false, toByteArray(1), toByteArray(0, 0, 0, 0, 0, 0, 0, 1))
        val readRow5 = Row(5, true, toByteArray(2), toByteArray(0, 0, 0, 0, 0, 0, 0, 2))

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", FloatType),
            StructField("c2", FloatType),
            StructField("c3", FloatType)
          )
        )

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2, readRow3, readRow4, readRow5), readSchema)
    }
  }

  test("Test Convert from java.lang.Double to BIT") {
    // success
    // java.lang.Double -> BIT
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val zero: java.lang.Double = 0d
        val one: java.lang.Double = 1d
        val a: java.lang.Double = 1.1d
        val b: java.lang.Double = 2.2d
        val c: java.lang.Double = 1.7976e+3
        val d: java.lang.Double = 4.9e-2

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, zero, b, c)
        val row3 = Row(3, one, a, d)
        val row4 = Row(4, zero, a, a)
        val row5 = Row(5, one, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DoubleType),
            StructField("c2", DoubleType),
            StructField("c3", DoubleType)
          )
        )

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, false, toByteArray(2), toByteArray(0, 0, 0, 0, 0, 0, 7, 6))
        val readRow3 = Row(3, true, toByteArray(1), toByteArray(0, 0, 0, 0, 0, 0, 0, 0))
        val readRow4 = Row(4, false, toByteArray(1), toByteArray(0, 0, 0, 0, 0, 0, 0, 1))
        val readRow5 = Row(5, true, toByteArray(2), toByteArray(0, 0, 0, 0, 0, 0, 0, 2))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(Seq(readRow1, readRow2, readRow3, readRow4, readRow5), readSchema)
    }
  }

  // TODO: test following types
  // java.lang.String
  // java.math.BigDecimal
  // java.sql.Date
  // java.sql.Timestamp
  // Array[String]
  // scala.collection.Seq
  // scala.collection.Map
  // org.apache.spark.sql.Row

  override def afterAll(): Unit = {
    TypeSystem.resetVersion()
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
  }
}
