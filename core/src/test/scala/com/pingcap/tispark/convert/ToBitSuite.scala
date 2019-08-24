package com.pingcap.tispark.convert

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

/**
 * BIT type include:
 * 1. BIT
 */
class ToBitSuite extends BaseDataSourceTest {

  private val readZero: java.lang.Long = 0L
  private val readOne: java.lang.Long = 1L
  private val readA: java.lang.Long = 1L
  private val readB: java.lang.Long = 2L
  private val readC: java.lang.Long = 3400L
  private val readD: java.lang.Long = 0L

  private val readRow1 = Row(1, null, null, null)
  private val readRow2 = Row(2, readZero, readB, readC)
  private val readRow3 = Row(3, readOne, readA, readD)
  private val readRow4 = Row(4, readZero, readA, readA)
  private val readRow5 = Row(5, readOne, readB, readB)

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", LongType),
      StructField("c2", LongType),
      StructField("c3", LongType)
    )
  )

  val tableTemplate = "test_%s_to_bit"
  val queryTemplate = "create table `%s`.`%s`(i INT, c1 BIT(1), c2 BIT(8),  c3 BIT(64))"

  test("Test Convert from java.lang.Boolean to BIT") {
    // success
    // java.lang.Boolean -> BIT
    val table = tableTemplate.format("boolean")
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Boolean = true
        val b: java.lang.Boolean = false

        val row1 = Row(1, null, null, null)
        val row2 = Row(2, a, b, a)
        val row3 = Row(3, b, a, b)
        val row4 = Row(4, a, a, a)
        val row5 = Row(5, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", BooleanType),
            StructField("c2", BooleanType),
            StructField("c3", BooleanType)
          )
        )

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, readOne, readZero, readOne)
        val readRow3 = Row(3, readZero, readOne, readZero)
        val readRow4 = Row(4, readOne, readOne, readOne)
        val readRow5 = Row(5, readZero, readZero, readZero)

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, table, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          table
        )
    }
  }

  test("Test Convert from java.lang.Byte to BIT") {
    // success
    // java.lang.Byte -> BIT
    val table = tableTemplate.format("boolean")
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

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ByteType),
            StructField("c2", ByteType),
            StructField("c3", ByteType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5), schema, table)
    }
  }

  test("Test Convert from java.lang.Short to BIT") {
    // success
    // java.lang.Short -> BIT
    val table = tableTemplate.format("boolean")
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

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ShortType),
            StructField("c2", ShortType),
            StructField("c3", ShortType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5), schema, table)
    }
  }

  test("Test Convert from java.lang.Integer to BIT") {
    // success
    // java.lang.Integer -> BIT
    val table = tableTemplate.format("boolean")
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

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", IntegerType),
            StructField("c2", IntegerType),
            StructField("c3", IntegerType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5), schema, table)
    }
  }

  test("Test Convert from java.lang.Long to BIT") {
    // success
    // java.lang.Long -> BIT
    val table = tableTemplate.format("boolean")
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

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType),
            StructField("c3", LongType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, table, None)
        compareTiDBSelectWithJDBC(Seq(row1, row2, row3, row4, row5), schema, table)
    }
  }

  test("Test Convert from java.lang.Float to BIT") {
    // success
    // java.lang.Float -> BIT
    val table = tableTemplate.format("boolean")
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

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", FloatType),
            StructField("c2", FloatType),
            StructField("c3", FloatType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, table, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          table
        )
    }
  }

  test("Test Convert from java.lang.Double to BIT") {
    // success
    // java.lang.Double -> BIT
    val table = tableTemplate.format("boolean")
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

        val readZero: java.lang.Long = 0L
        val readOne: java.lang.Long = 1L
        val readA: java.lang.Long = 1L
        val readB: java.lang.Long = 2L
        val readC: java.lang.Long = 1798L
        val readD: java.lang.Long = 0L

        val readRow1 = Row(1, null, null, null)
        val readRow2 = Row(2, readZero, readB, readC)
        val readRow3 = Row(3, readOne, readA, readD)
        val readRow4 = Row(4, readZero, readA, readA)
        val readRow5 = Row(5, readOne, readB, readB)

        val readSchema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType),
            StructField("c3", LongType)
          )
        )

        dropTable(table)
        createTable(queryTemplate, table)

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, table, None)
        compareTiDBSelectWithJDBC(
          Seq(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          table
        )
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
}
