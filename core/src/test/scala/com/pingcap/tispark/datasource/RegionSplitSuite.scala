package com.pingcap.tispark.datasource

import com.pingcap.tikv.TiBatchWriteUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class RegionSplitSuite extends BaseDataSourceTest("region_pre_split_test") {
  private val row1 = Row(1)
  private val schema = StructType(
    List(
      StructField("a", IntegerType)
    )
  )

  test("region pre split test") {
    // do test this case on tidb which does not support split region
    if (!isEnableSplitRegion) {
      cancel
    }

    dropTable()
    jdbcUpdate(
      s"CREATE TABLE  $dbtable ( `a` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
    )

    val dbName = "tidb_tispark_test"
    val tableName = "region_pre_split_test"
    val options = Some(Map("enableRegionSplit" -> "true", "regionSplitNum" -> "3"))
    tidbWrite(List(row1), schema, options)
    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbName, tableName)
    val regionsNum = TiBatchWriteUtils.getRegionsByTable(ti.tiSession, tiTableInfo).size()
    assert(regionsNum == 3)
  }
}
