/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.clustered

import com.pingcap.tispark.TiConfigConst
import com.pingcap.tispark.test.generator.DataType.INT
import com.pingcap.tispark.test.generator.NullableType

class IndexScan0Suite extends ClusteredIndexTest {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sqlContext.setConf(TiConfigConst.USE_INDEX_SCAN_FIRST, "true")
  }

  override def afterAll(): Unit = {
    spark.sqlContext.setConf(TiConfigConst.USE_INDEX_SCAN_FIRST, "false")
    super.afterAll()
  }

  test("index scan 0: primary key has one column") {
    if (!supportClusteredIndex) {
      cancel("currently tidb instance does not support clustered index")
    }
    for (dataType1 <- testDataTypes1) {
      for (dataType2 <- testDataTypes2) {
        val schemaAndDataList = genSchemaAndData(
          rowCount,
          List(dataType2, dataType1, INT).map(d =>
            genDescription(d, NullableType.NumericNotNullable)),
          database,
          isClusteredIndex = true,
          hasTiFlashReplica = enableTiFlashTest)
        schemaAndDataList.foreach { schema =>
          test(schema)
        }
      }
    }
  }

  test("cluster index scan : primary key has multiply column") {
    if (!supportClusteredIndex) {
      cancel("currently tidb instance does not support clustered index")
    }
    for (dataType1 <- testDataTypes1) {
      for (dataType2 <- testDataTypes2) {
        val schemaAndDataList = genSchemaAndData(
          rowCount,
          List(dataType2, dataType1, dataType2, dataType1, INT).map(d =>
            genDescription(d, NullableType.NumericNotNullable)),
          database,
          isClusteredIndex = true,
          hasTiFlashReplica = enableTiFlashTest)
        schemaAndDataList.foreach { schema =>
          test(schema)
        }
      }
    }
  }
}
