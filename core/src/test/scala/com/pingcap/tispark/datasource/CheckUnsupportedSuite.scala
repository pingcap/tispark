package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class CheckUnsupportedSuite extends BaseDataSourceSuite("test_datasource_check_unsupported") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

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
      s"create table $dbtableInJDBC(i int, s varchar(128), UNIQUE (i))"
    )
    jdbcUpdate(
      s"insert into $dbtableInJDBC values(null, 'Hello')"
    )

    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals(
            "tispark currently does not support write data table with secondary index(KEY & UNIQUE KEY)!"
          )
      )
    }

    testSelect(dbtableInSpark, Seq(row1))
  }

  test("Test write to table with secondary index: KEY") {
    dropTable()

    jdbcUpdate(
      s"create table $dbtableInJDBC(i int, s varchar(128), KEY (s))"
    )
    jdbcUpdate(
      s"insert into $dbtableInJDBC values(null, 'Hello')"
    )

    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals(
            "tispark currently does not support write data table with secondary index(KEY & UNIQUE KEY)!"
          )
      )
    }

    testSelect(dbtableInSpark, Seq(row1))
  }

  test("Test write to partition table") {
    dropTable()

    jdbcUpdate(
      s"create table $dbtableInJDBC(i int, s varchar(128)) partition by range(i) (partition p0 values less than maxvalue)"
    )
    jdbcUpdate(
      s"insert into $dbtableInJDBC values(null, 'Hello')"
    )

    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals("tispark currently does not support write data to partition table!")
      )
    }

    testSelect(dbtableInSpark, Seq(row1))
  }

  test(
    "Test write to table with primary key & primary key is not handle (TINYINT、SMALLINT、MEDIUMINT、INTEGER)"
  ) {
    dropTable()

    jdbcUpdate(
      s"create table $dbtableInJDBC(i int, s varchar(128), primary key(s))"
    )
    jdbcUpdate(
      s"insert into $dbtableInJDBC values(null, 'Hello')"
    )

    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals(
            "tispark currently does not support write data to table with primary key, but type is not TINYINT、SMALLINT、MEDIUMINT、INTEGER!"
          )
      )
    }

    testSelect(dbtableInSpark, Seq(row1))
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
