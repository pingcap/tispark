/*
 *
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
 *
 */

package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseInitialOnceSuite

class ComplexAggregateSuite extends BaseInitialOnceSuite {
  private val allCases = Seq[String](
    "select min(tp_smallint) from full_data_type_table",
    "select sum(tp_char) from full_data_type_table",
    "select sum(tp_double) from full_data_type_table",
    "select min(tp_tinyint) from full_data_type_table",
    "select avg(tp_int) from full_data_type_table",
    "select max(tp_bigint) from full_data_type_table"
  )

  allCases.map { _.replace(")", " / tp_int)") } ++ allCases.map { _.replace(")", " / tp_double)") } ++ allCases
    .map { _.replace(")", " + tp_float * 2)") } foreach { query =>
    test(query) {
      runTest(query)
    }
  }
}
