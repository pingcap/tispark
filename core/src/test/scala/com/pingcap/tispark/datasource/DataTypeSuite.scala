package com.pingcap.tispark.datasource

import java.sql.{Date, Timestamp}
import java.util.Calendar

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, CharType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType, VarcharType}

class DataTypeSuite extends BaseDataSourceTest("test_datasource_datatype") {

  //todo support TIME/YEAR/BINARY/SET
  test("Test different data type") {
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i INT primary key,
                  |c1 BIT(1),
                  |c2 BIT(8),
                  |c3 TINYINT,
                  |c4 BOOLEAN,
                  |c5 SMALLINT,
                  |c6 MEDIUMINT,
                  |c7 INTEGER,
                  |c8 BIGINT,
                  |c9 FLOAT,
                  |c10 DOUBLE,
                  |c11 DECIMAL(38,18),
                  |c12 DATE,
                  |c13 DATETIME,
                  |c14 TIMESTAMP,
                  |c17 CHAR(64),
                  |c18 VARCHAR(64),
                  |c20 VARBINARY(64),
                  |c21 TINYBLOB,
                  |c22 TINYTEXT,
                  |c23 BLOB,
                  |c24 TEXT,
                  |c25 MEDIUMBLOB,
                  |c26 MEDIUMTEXT,
                  |c27 LONGBLOB,
                  |c28 LONGTEXT,
                  |c29 ENUM('male' , 'female' , 'both' , 'unknown')
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("c1", ByteType),
        StructField("c2", ByteType),
        StructField("c3", IntegerType),
        StructField("c4", IntegerType),
        StructField("c5", IntegerType),
        StructField("c6", IntegerType),
        StructField("c7", IntegerType),
        StructField("c8", LongType),
        StructField("c9", FloatType),
        StructField("c10", DoubleType),
        StructField("c11", DecimalType.SYSTEM_DEFAULT),
        StructField("c12", StringType),
        StructField("c13", LongType),
        StructField("c14", LongType),
        //StructField("c15", ),
        //StructField("c16", ),
        StructField("c17", StringType),
        StructField("c18", StringType),
        //StructField("c19", StringType),
        StructField("c20", StringType),
        StructField("c21", StringType),
        StructField("c22", StringType),
        StructField("c23", StringType),
        StructField("c24", StringType),
        StructField("c25", StringType),
        StructField("c26", StringType),
        StructField("c27", StringType),
        StructField("c28", StringType),
        StructField("c29", StringType)
        //StructField("c30", StringType),
      )
    )
    val timestampInLong = Calendar.getInstance().getTimeInMillis
    val row1 = Row(
      1,
      0.toByte,
      18.toByte,
      29,
      1,
      28,
      128,
      256,
      11111111111L,
      12.23f,
      23.456,
      BigDecimal(1.23),
      "2019-06-10",
      timestampInLong,
      timestampInLong,
      "PingCap",
      "TiSpark",
      "Tidb varbinary",
      "Tidb tinyblob",
      "Tidb tinytext",
      "Tidb blob",
      "Tidb text",
      "Tidb mediumblob",
      "Tidb mediumtext",
      "Tidb longblob",
      "Tidb longtext",
      "male"
    )
    val row2 = Row(
      2,
      1.toByte,
      18.toByte,
      29,
      1,
      28,
      128,
      256,
      21111111111L,
      12.33f,
      23.457,
      BigDecimal(1.24),
      "2019-06-10",
      timestampInLong,
      timestampInLong,
      "PingCap",
      "TiSpark",
      "Tidb varbinary",
      "Tidb tinyblob",
      "Tidb tinytext",
      "Tidb blob",
      "Tidb text",
      "Tidb mediumblob",
      "Tidb mediumtext",
      "Tidb longblob",
      "Tidb longtext",
      "female"
    )
    val data = List(row1, row2)
    tidbWrite(data, schema)
    val timestamp = new Timestamp(timestampInLong)
    val row3 = Row(
      1,
      0.toByte,
      18.toByte,
      29,
      1,
      28,
      128,
      256,
      11111111111L,
      12.23f,
      23.456,
      BigDecimal(1.23),
      Date.valueOf("2019-06-10"),
      timestamp,
      timestamp,
      "PingCap",
      "TiSpark",
      "Tidb varbinary".toArray,
      "Tidb tinyblob".toArray,
      "Tidb tinytext",
      "Tidb blob".toArray,
      "Tidb text",
      "Tidb mediumblob".toArray,
      "Tidb mediumtext",
      "Tidb longblob".toArray,
      "Tidb longtext",
      "male"
    )
    val row4 = Row(
      2,
      1.toByte,
      18.toByte,
      29,
      1,
      28,
      128,
      256,
      21111111111L,
      12.33f,
      23.457,
      BigDecimal(1.24),
      Date.valueOf("2019-06-10"),
      timestamp,
      timestamp,
      "PingCap",
      "TiSpark",
      "Tidb varbinary".toArray,
      "Tidb tinyblob".toArray,
      "Tidb tinytext",
      "Tidb blob".toArray,
      "Tidb text",
      "Tidb mediumblob".toArray,
      "Tidb mediumtext",
      "Tidb longblob".toArray,
      "Tidb longtext",
      "female"
    )
    val ref = List(row3, row4)
    testTiDBSelect(ref)
  }

  test("Test integer pk") {
    // integer pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i INT primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row(1, "test")
    val row2 = Row(2, "spark")
    var data = List(row1, row2)
    tidbWrite(data, schema)
    testTiDBSelect(data)

    val row3 = Row(1, "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test decimal pk") {
    // decimal pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i decimal(3,2) primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", DecimalType(3, 2)),
        StructField("c1", StringType)
      )
    )
    val row1 = Row(BigDecimal(1.23), "test")
    val row2 = Row(BigDecimal(1.24), "spark")
    var data = List(row1, row2)
    tidbWrite(data, schema)
    testTiDBSelect(data)

    val row3 = Row(BigDecimal(1.23), "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test char pk") {
    // char pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i char(4) primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("row1", "test")
    val row2 = Row("row2", "spark")
    var data = List(row1, row2)
    tidbWrite(data, schema)
    testTiDBSelect(data)

    val row3 = Row("row1", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test varchar pk") {
    // char pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i varchar(4) primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("r1", "test")
    val row2 = Row("r2", "spark")
    var data = List(row1, row2)
    tidbWrite(data, schema)
    testTiDBSelect(data)

    val row3 = Row("r1", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test date pk") {
    // char pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i date primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("2019-06-10", "test")
    val row2 = Row("2019-06-11", "spark")
    var data = List(row1, row2)
    val ref =
      List(Row(Date.valueOf("2019-06-10"), "test"), Row(Date.valueOf("2019-06-11"), "spark"))
    tidbWrite(data, schema)
    testTiDBSelect(ref)

    val row3 = Row("2019-06-10", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test datetime pk") {
    // datetime pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i DATETIME primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("c1", StringType)
      )
    )
    val timeInLong = Calendar.getInstance().getTimeInMillis
    val timeInLong1 = timeInLong + 12345
    val row1 = Row(timeInLong, "test")
    val row2 = Row(timeInLong1, "spark")
    var data = List(row1, row2)
    val ref = List(Row(new Timestamp(timeInLong), "test"), Row(new Timestamp(timeInLong1), "spark"))
    tidbWrite(data, schema)
    testTiDBSelect(ref)

    val row3 = Row(timeInLong, "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test timestamp pk") {
    // timestamp pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i timestamp primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", LongType),
        StructField("c1", StringType)
      )
    )
    val timeInLong = Calendar.getInstance().getTimeInMillis
    val timeInLong1 = timeInLong + 12345
    val row1 = Row(timeInLong, "test")
    val row2 = Row(timeInLong1, "spark")
    var data = List(row1, row2)
    val ref = List(Row(new Timestamp(timeInLong), "test"), Row(new Timestamp(timeInLong1), "spark"))
    tidbWrite(data, schema)
    testTiDBSelect(ref)

    val row3 = Row(timeInLong, "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test text pk") {
    // timestamp pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i text,
                  |c1 varchar(64),
                  |primary key(i(128))
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("row1", "test")
    val row2 = Row("row2", "spark")
    var data = List(row1, row2)
    tidbWrite(data, schema)
    testTiDBSelect(data)

    val row3 = Row("row1", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test blob pk") {
    // blob pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i blob,
                  |c1 varchar(64),
                  |primary key(i(128))
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("row1", "test")
    val row2 = Row("row2", "spark")
    var data = List(row1, row2)
    val ref = List(Row("row1".toArray, "test"), Row("row2".toArray, "spark"))
    tidbWrite(data, schema)
    testTiDBSelect(ref)

    val row3 = Row("row1", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test enum pk") {
    // enum pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i ENUM('male','female','both','unknown') primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("male", "test")
    val row2 = Row("female", "spark")
    var data = List(row2, row1)
    tidbWrite(data, schema)
    testTiDBSelect(data)

    val row3 = Row("male", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  test("Test composite pk") {
    // enum pk
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i int,
                  |i1 int,
                  |c1 varchar(64),
                  |primary key(i,i1)
                  |)
      """.stripMargin)
    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("i1", IntegerType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row(1, 2, "test")
    val row2 = Row(2, 2, "tispark")
    var data = List(row1, row2)
    tidbWrite(data, schema)
    testTiDBSelect(data)

    val row3 = Row(1, 2, "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWrite(data, schema)
    }
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
