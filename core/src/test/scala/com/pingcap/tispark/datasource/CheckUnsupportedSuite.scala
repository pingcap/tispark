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

  test("Test write to partition table") {
    dropTable()

    tidbStmt.execute("set @@tidb_enable_table_partition = 1")

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

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
