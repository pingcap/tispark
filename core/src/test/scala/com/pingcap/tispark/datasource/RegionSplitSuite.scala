/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.datasource

import com.pingcap.tikv.TiBatchWriteUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class RegionSplitSuite extends BaseDataSourceTest("region_split_test") {
  private val row1 = Row(1)
  private val row2 = Row(2)
  private val row3 = Row(3)
  private val schema = StructType(List(StructField("a", IntegerType)))

  test("index region split test") {
    if (!supportBatchWrite) {
      cancel
    }

    // do not test this case on tidb which does not support split region
    if (!isEnableSplitRegion) {
      cancel
    }

    dropTable()
    jdbcUpdate(
      s"CREATE TABLE  $dbtable ( `a` int(11), unique index(a)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")

    val options = Some(
      Map("enableRegionSplit" -> "true", "regionSplitMethod" -> "v1", "regionSplitNum" -> "3"))

    tidbWrite(List(row1, row2, row3), schema, options)

    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbPrefix + database, table)
    val regionsNum = TiBatchWriteUtils
      .getRegionByIndex(ti.tiSession, tiTableInfo, tiTableInfo.getIndices.get(0))
      .size()
    assert(regionsNum == 3)
  }

  test("table region split test") {
    if (!supportBatchWrite) {
      cancel
    }

    // do not test this case on tidb which does not support split region
    if (!isEnableSplitRegion) {
      cancel
    }

    dropTable()
    jdbcUpdate(
      s"CREATE TABLE  $dbtable ( `a` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")

    val options = Some(
      Map("enableRegionSplit" -> "true", "regionSplitMethod" -> "v1", "regionSplitNum" -> "3"))

    tidbWrite(List(row1, row2, row3), schema, options)

    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbPrefix + database, table)
    val regionsNum = TiBatchWriteUtils.getRecordRegions(ti.tiSession, tiTableInfo).size()
    assert(regionsNum == 3)
  }
}
