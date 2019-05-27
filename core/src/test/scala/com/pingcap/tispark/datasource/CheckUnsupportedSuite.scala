package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class CheckUnsupportedSuite extends BaseDataSourceTest("test_datasource_check_unsupported") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")

  private val schema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  override def beforeAll(): Unit =
    super.beforeAll()

  test("Test write to table with secondary index: UNIQUE") {
    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(128), UNIQUE (i))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello')"
    )

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals(
            "tispark currently does not support write data table with secondary index(KEY & UNIQUE KEY)!"
          )
      )
    }

    testTiDBSelect(Seq(row1))
  }

  test("Test write to table with secondary index: KEY") {
    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(128), KEY (s))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello')"
    )

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals(
            "tispark currently does not support write data table with secondary index(KEY & UNIQUE KEY)!"
          )
      )
    }

    testTiDBSelect(Seq(row1))
  }

  test("Test write to partition table") {
    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(128)) partition by range(i) (partition p0 values less than maxvalue)"
    )
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello')"
    )

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals("tispark currently does not support write data to partition table!")
      )
    }

    testTiDBSelect(Seq(row1))
  }

  test(
    "Test write to table with primary key & primary key is not handle (TINYINT、SMALLINT、MEDIUMINT、INTEGER)"
  ) {
    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(128), primary key(s))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello')"
    )

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals(
            "tispark currently does not support write data to table with primary key, but type is not TINYINT、SMALLINT、MEDIUMINT、INTEGER!"
          )
      )
    }

    testTiDBSelect(Seq(row1))
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
