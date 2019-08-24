package com.pingcap.tispark.datatype

import java.sql.{Date, Timestamp}
import java.util.Calendar

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class DataTypeSuite extends BaseDataSourceTest("test") {

  test("Test Read different types") {
    val table = "read_different_types"
    dropTable(table)
    createTable(
      s"""
         |create table `%s`.`%s`(
         |i INT,
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
         |c11 DECIMAL,
         |c12 DATE,
         |c13 DATETIME,
         |c14 TIMESTAMP,
         |c15 TIME,
         |c16 YEAR,
         |c17 CHAR(64),
         |c18 VARCHAR(64),
         |c19 BINARY(64),
         |c20 VARBINARY(64),
         |c21 TINYBLOB,
         |c22 TINYTEXT,
         |c23 BLOB,
         |c24 TEXT,
         |c25 MEDIUMBLOB,
         |c26 MEDIUMTEXT,
         |c27 LONGBLOB,
         |c28 LONGTEXT,
         |c29 ENUM('male' , 'female' , 'both' , 'unknow'),
         |c30 SET('a', 'b', 'c')
         |)
      """.stripMargin,
      table
    )

    jdbcUpdate(
      """
        |insert into `%s`.`%s` values(
        |1,
        |B'1',
        |B'01111100',
        |29,
        |1,
        |29,
        |29,
        |29,
        |29,
        |29.9,
        |29.9,
        |29.9,
        |'2019-01-01',
        |'2019-01-01 11:11:11',
        |'2019-01-01 11:11:11',
        |'11:11:11',
        |'2019',
        |'char test',
        |'varchar test',
        |'binary test',
        |'varbinary test',
        |'tinyblob test',
        |'tinytext test',
        |'blob test',
        |'text test',
        |'mediumblob test',
        |'mediumtext test',
        |'longblob test',
        |'longtext test',
        |'male',
        |'a,b'
        |)
       """.stripMargin,
      table
    )

    val tiTableInfo = getTableInfo(database, table)
    for (i <- 0 until tiTableInfo.getColumns.size()) {
      println(s"$i -> ${tiTableInfo.getColumn(i).getType}")
    }
  }

  //todo support TIME/YEAR/BINARY/SET
  test("Test different data type") {
    val table = "different_data_type"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
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
      """.stripMargin,
      table
    )
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
        StructField("c13", TimestampType),
        StructField("c14", TimestampType),
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
    val timestamp = new Timestamp(Calendar.getInstance().getTimeInMillis)
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
      timestamp,
      timestamp,
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
      timestamp,
      timestamp,
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
    tidbWriteWithTable(data, schema, table)
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
    testTiDBSelectWithTable(ref, tableName = table)
  }

  test("Test integer pk") {
    // integer pk
    val table = "int_pk_table"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i INT primary key,
        |c1 varchar(64)
        |)
      """.stripMargin,
      table
    )
    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row(1, "test")
    val row2 = Row(2, "spark")
    var data = List(row1, row2)
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row(1, "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test decimal pk") {
    // decimal pk
    val table = "decimal_pk_tbl"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i decimal(3,2) primary key,
        |c1 varchar(64)
        |)
      """.stripMargin,
      table
    )
    val schema = StructType(
      List(
        StructField("i", DecimalType(3, 2)),
        StructField("c1", StringType)
      )
    )
    val row1 = Row(BigDecimal(1.23), "test")
    val row2 = Row(BigDecimal(1.24), "spark")
    var data = List(row1, row2)
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row(BigDecimal(1.23), "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test char pk") {
    // char pk
    val table = "char_pk_tbl"
    dropTable(table)
    createTable(s"""
                  |create table `%s`.`%s`(
                  |i char(4) primary key,
                  |c1 varchar(64)
                  |)
      """.stripMargin, table)
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("row1", "test")
    val row2 = Row("row2", "spark")
    var data = List(row1, row2)
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row("row1", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test varchar pk") {
    // char pk
    val table = "varchat_pk_tbl"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i varchar(4) primary key,
        |c1 varchar(64)
        |)
      """.stripMargin,
      table
    )
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("r1", "test")
    val row2 = Row("r2", "spark")
    var data = List(row1, row2)
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row("r1", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test date pk") {
    // char pk
    val table = "date_pk_tbl"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i date primary key,
        |c1 varchar(64)
        |)
      """.stripMargin,
      table
    )
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
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(ref, tableName = table)

    val row3 = Row("2019-06-10", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test datetime pk") {
    // datetime pk
    val table = "datetime_pk_tbl"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i DATETIME primary key,
        |c1 varchar(64)
        |)
      """.stripMargin,
      table
    )
    val schema = StructType(
      List(
        StructField("i", TimestampType),
        StructField("c1", StringType)
      )
    )
    val timeInLong = Calendar.getInstance().getTimeInMillis
    val timeInLong1 = timeInLong + 12345
    val row1 = Row(new Timestamp(timeInLong), "test")
    val row2 = Row(new Timestamp(timeInLong1), "spark")
    var data = List(row1, row2)
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row(new Timestamp(timeInLong), "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test timestamp pk") {
    // timestamp pk
    val table = "ts_pk_tbl"
    dropTable(table)
    createTable(
      s"""
         |create table `%s`.`%s`(
         |i timestamp primary key,
         |c1 varchar(64)
         |)
      """.stripMargin,
      table
    )
    val schema = StructType(
      List(
        StructField("i", TimestampType),
        StructField("c1", StringType)
      )
    )
    val timeInLong = Calendar.getInstance().getTimeInMillis
    val timeInLong1 = timeInLong + 12345
    val row1 = Row(new Timestamp(timeInLong), "test")
    val row2 = Row(new Timestamp(timeInLong1), "spark")
    var data = List(row1, row2)
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row(new Timestamp(timeInLong), "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test text pk") {
    // text pk
    val table = "text_pk_tbl"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i text,
        |c1 varchar(64),
        |primary key(i(128))
        |)
      """.stripMargin,
      table
    )
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("row1", "test")
    val row2 = Row("row2", "spark")
    var data = List(row1, row2)
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row("row1", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test blob pk") {
    // blob pk
    val table = "blob_pk_tbl"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i blob,
        |c1 varchar(64),
        |primary key(i(128))
        |)
      """.stripMargin,
      table
    )
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
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(ref, tableName = table)

    val row3 = Row("row1", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test enum pk") {
    // enum pk
    val table = "enum_pk_tbl"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i ENUM('male','female','both','unknown') primary key,
        |c1 varchar(64)
        |)
      """.stripMargin,
      table
    )
    val schema = StructType(
      List(
        StructField("i", StringType),
        StructField("c1", StringType)
      )
    )
    val row1 = Row("male", "test")
    val row2 = Row("female", "spark")
    var data = List(row2, row1)
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row("male", "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }

  test("Test composite pk") {
    // enum pk
    val table = "composite_pk_tbl"
    dropTable(table)
    createTable(
      """
        |create table `%s`.`%s`(
        |i int,
        |i1 int,
        |c1 varchar(64),
        |primary key(i,i1)
        |)
      """.stripMargin,
      table
    )
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
    tidbWriteWithTable(data, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    val row3 = Row(1, 2, "spark")
    data = List(row1, row3)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(data, schema, table)
    }
  }
}
