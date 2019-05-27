package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class TiSparkTypeSuite extends BaseDataSourceSuite("type_test") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2L, "TiDB")
  private val row3 = Row(3L, "Spark")
  private val row5 = Row(Long.MaxValue, "Duplicate")

  private val schema = StructType(
    List(
      StructField("i", LongType),
      StructField("s", StringType)
    )
  )
  test("bigint test") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i bigint, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB')"
    )

    batchWrite(List(row3, row5), schema)
    testSelect(List(row1, row2, row3, row5))
  }
}
