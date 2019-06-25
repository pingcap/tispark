/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tispark

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

class IndexReadSuite extends BaseTiSparkTest {

  private val database = "tispark_test"
  private val table = "test_timezone"

  test("Test double read 2") {

    sql(
      s"explain select i, c1 from index_read3 where c1 = '10417'"
    ).show(400, false)

    /*println(
      sql(
        s"select i, c1 from index_read2 where c1 = '10417'"
      ).count()
    )*/

  }

}
