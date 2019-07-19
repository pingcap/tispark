package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class ShardRowIDBitsSuite extends BaseDataSourceTest("test_shard_row_id_bits") {
  private val row1 = Row(1)
  private val row2 = Row(2)
  private val row3 = Row(3)
  private val schema = StructType(
    List(
      StructField("a", IntegerType)
    )
  )

  test("reading from a table with shard_row_id_bits") {
    dropTable()
    jdbcUpdate(
      s"CREATE TABLE  $dbtable ( `a` int(11))  SHARD_ROW_ID_BITS = 4"
    )

    jdbcUpdate(
      s"insert into $dbtable values(null)"
    )

    tidbWrite(List(row1, row2, row3), schema)
    testTiDBSelect(List(Row(null), row1, row2, row3), sortCol = "a")
  }
}
