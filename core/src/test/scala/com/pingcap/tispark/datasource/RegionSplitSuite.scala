package com.pingcap.tispark.datasource

import com.pingcap.tikv.TiBatchWriteUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class RegionSplitSuite extends BaseDataSourceTest {
  private val row1 = Row(1)
  private val row2 = Row(2)
  private val row3 = Row(3)
  private val schema = StructType(
    List(
      StructField("a", IntegerType)
    )
  )

  test("index region split test") {
    // do not test this case on tidb which does not support split region
    val table = "index_region_split_test"
    if (!isEnableSplitRegion) {
      cancel
    }

    dropTable(table)
    createTable(
      "CREATE TABLE  `%s`.`%s` ( `a` int(11), unique index(a)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
      table
    )

    val options = Some(Map("enableRegionSplit" -> "true", "regionSplitNum" -> "3"))

    tidbWriteWithTable(List(row1, row2, row3), schema, table, options)

    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbPrefix + database, table)
    val regionsNum = TiBatchWriteUtils
      .getRegionByIndex(ti.tiSession, tiTableInfo, tiTableInfo.getIndices.get(0))
      .size()
    assert(regionsNum == 3)
  }

  test("table region split test") {
    // do not test this case on tidb which does not support split region
    if (!isEnableSplitRegion) {
      cancel
    }
    val table = "tbl_region_split_test"
    dropTable(table)
    createTable(
      "CREATE TABLE  `%s`.`%s` ( `a` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
      table
    )

    val options = Some(Map("enableRegionSplit" -> "true", "regionSplitNum" -> "3"))

    tidbWriteWithTable(List(row1, row2, row3), schema, table, options)

    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbPrefix + database, table)
    val regionsNum = TiBatchWriteUtils.getRegionsByTable(ti.tiSession, tiTableInfo).size()
    assert(regionsNum == 3)
  }
}
