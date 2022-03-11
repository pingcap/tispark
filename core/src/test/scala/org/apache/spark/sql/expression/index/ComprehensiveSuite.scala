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

package org.apache.spark.sql.expression.index

import org.apache.spark.sql.BaseInitialOnceTest

class ComprehensiveSuite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    // MultiKeys
    """select tp_int, tp_float from full_data_type_table_idx
      | where tp_int = 1690889031 and tp_int = 1431435712
      | order by 1, 2""".stripMargin,
    """select tp_int, tp_float from full_data_type_table_idx
      | where tp_int = 1690889031 or tp_int = 1431435712
      | order by 1, 2""".stripMargin,
    """select tp_int, tp_float from full_data_type_table_idx
      | where tp_float > 0.000001 and tp_float <= 3.1415925 and tp_int = 1690889031 and tp_int = 1690889031
      | order by 1, 2""".stripMargin,
    // Empty set
    """select count(*) from full_data_type_table_idx
      | where tp_float < 0.000001 and tp_float > 3.1415925""".stripMargin,
    """select tp_float, tp_int from full_data_type_table_idx
      | where tp_float < 0.000001 and tp_float > 1.1415925 and tp_int = 1690889031
      | order by 1,2""".stripMargin,
    // OR
    """select tp_float, tp_int from full_data_type_table_idx
      | where (tp_float < 0.000001 or tp_float > 0.1415925) and (tp_int = 2333 or tp_int = 1238733782)
      | order by 1,2""".stripMargin,
    // Is null
    "select count(*) from full_data_type_table_idx where tp_int != 0",
    "select count(*) from full_data_type_table_idx where tp_int is null",
    "select count(*) from full_data_type_table_idx where tp_int = null",
    "select count(*) from full_data_type_table_idx where tp_int is not null",
    "select count(*) from full_data_type_table_idx where tp_int != 0 or tp_int is null",
    """select id_dt from full_data_type_table_idx
      | where tp_int is null and tp_tinyint < 100 order by 1""".stripMargin,
    """select id_dt from full_data_type_table_idx
      | where (tp_int is null or tp_int = 4355836469450447576) and tp_tinyint < 100 order by 1""".stripMargin)

  test("Test index - Comprehensive") {
    allCases.foreach { query =>
      runTest(query)
    }
  }
}
