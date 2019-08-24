package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class TiSparkTypeSuite extends BaseDataSourceTest {
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
    val table = "big_int_test"
    dropTable(table)
    createTable(s"create table `%s`.`%s`(i bigint, s varchar(128))", table)
    jdbcUpdate(
      "insert into `%s`.`%s` values(null, 'Hello'), (2, 'TiDB')",
      table
    )

    tidbWriteWithTable(List(row3, row5), schema, table)
    testTiDBSelectWithTable(List(row1, row2, row3, row5), tableName = table)
  }
}
