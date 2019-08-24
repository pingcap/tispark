package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class ShardRowIDBitsSuite extends BaseDataSourceTest {
  private val row1 = Row(1)
  private val row2 = Row(2)
  private val row3 = Row(3)
  private val schema = StructType(
    List(
      StructField("a", IntegerType)
    )
  )

  test("reading and writing a table with shard_row_id_bits") {
    val table = "show_row_id_bits_test"
    dropTable(table)
    createTable(
      s"CREATE TABLE  `%s`.`%s` ( `a` int(11))  SHARD_ROW_ID_BITS = 4",
      table
    )

    jdbcUpdate(
      s"insert into `%s`.`%s` values(null)",
      table
    )

    tidbWriteWithTable(List(row1, row2, row3), schema, table)
    testTiDBSelectWithTable(List(Row(null), row1, row2, row3), sortCol = "a", tableName = table)
  }
}
