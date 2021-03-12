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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.clustered

import org.apache.spark.sql.test.generator.DataType.INT

class TableScan1Suite extends ClusteredIndexTest {
  test("table scan 1: primary key has two columns") {
    if (!supportClusteredIndex) {
      cancel("currently tidb instance does not support clustered index")
    }
    for (dataType1 <- testDataTypes) {
      for (dataType2 <- testDataTypes) {
        val schemas = genSchema(List(dataType2, dataType1, INT, INT), tablePrefix)
        schemas.foreach { schema =>
          test(schema)
        }
      }
    }
  }
}
