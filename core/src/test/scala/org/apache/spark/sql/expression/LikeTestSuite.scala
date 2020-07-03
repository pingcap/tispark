/*
 * Copyright 2017 PingCAP, Inc.
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

package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseInitialOnceTest

class LikeTestSuite extends BaseInitialOnceTest {
  private val tableScanCases = Seq[String](
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'a%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE '%a%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE '%f'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'a%a%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'a%f'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE '%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE '%dfs%df%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'ÿÿ%'")

  private val indexScanCases = Seq[String](
    "select tp_varchar from full_data_type_table_idx where tp_varchar LIKE '%'",
    "select tp_varchar from full_data_type_table_idx where tp_varchar LIKE 'a%f'",
    "select tp_varchar from full_data_type_table_idx where tp_varchar LIKE 'ÿ%'",
    "select * from full_data_type_table_idx where tp_varchar LIKE '%f'",
    "select * from full_data_type_table_idx where tp_varchar LIKE 'a%f'",
    "select * from full_data_type_table_idx where tp_varchar LIKE 'ÿ%'")

  test("Test - Like") {
    tableScanCases.foreach { query =>
      explainAndRunTest(query)
    }
    indexScanCases.foreach { query =>
      explainAndRunTest(query)
    }
  }
}
