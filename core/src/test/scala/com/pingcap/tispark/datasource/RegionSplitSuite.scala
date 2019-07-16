package com.pingcap.tispark.datasource

import com.pingcap.tikv.TiBatchWriteUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class RegionSplitSuite extends BaseDataSourceTest("region_split_test") {
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
    if (!isEnableSplitRegion) {
      cancel
    }

    dropTable()
    jdbcUpdate(
      s"CREATE TABLE  $dbtable ( `a` int(11), unique index(a)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
    )

    val options = Some(Map("enableRegionSplit" -> "true", "regionSplitNum" -> "3"))

    tidbWrite(List(row1, row2, row3), schema, options)

    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbPrefix + database, table)
    val regionsNum = TiBatchWriteUtils
      .getRegionByIndex(ti.tiSession, tiTableInfo, tiTableInfo.getIndices.get(0))
      .size()
    assert(regionsNum == 4)
  }

  test("table region split test") {
    // do not test this case on tidb which does not support split region
    if (!isEnableSplitRegion) {
      cancel
    }

    dropTable()
    jdbcUpdate(
      s"CREATE TABLE  $dbtable ( `a` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
    )

    val options = Some(Map("enableRegionSplit" -> "true", "regionSplitNum" -> "3"))

    tidbWrite(List(row1), schema, options)

    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbPrefix + database, table)
    val regionsNum = TiBatchWriteUtils.getRegionsByTable(ti.tiSession, tiTableInfo).size()
    assert(regionsNum == 3)
  }
}
