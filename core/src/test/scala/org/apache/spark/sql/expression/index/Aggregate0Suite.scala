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

package org.apache.spark.sql.expression.index

import org.apache.spark.sql.BaseTiSparkSuite

class Aggregate0Suite extends BaseTiSparkSuite {
  private val allCases = Seq[String](
    "select tp_mediumint from full_data_type_table_idx  group by (tp_mediumint)  order by tp_mediumint ",
    "select tp_float from full_data_type_table_idx  group by (tp_float)  order by tp_float ",
    "select tp_tinyint from full_data_type_table_idx  group by (tp_tinyint)  order by tp_tinyint ",
    "select tp_varchar from full_data_type_table_idx  group by (tp_varchar)  order by tp_varchar ",
    "select tp_datetime from full_data_type_table_idx  group by (tp_datetime)  order by tp_datetime ",
    "select tp_double from full_data_type_table_idx  group by (tp_double)  order by tp_double ",
    "select tp_int from full_data_type_table_idx  group by (tp_int)  order by tp_int ",
    "select tp_bigint from full_data_type_table_idx  group by (tp_bigint)  order by tp_bigint ",
    "select tp_timestamp from full_data_type_table_idx  group by (tp_timestamp)  order by tp_timestamp ",
    "select tp_real from full_data_type_table_idx  group by (tp_real)  order by tp_real ",
    "select tp_smallint from full_data_type_table_idx  group by (tp_smallint)  order by tp_smallint ",
    "select tp_nvarchar from full_data_type_table_idx  group by (tp_nvarchar)  order by tp_nvarchar ",
    "select tp_decimal from full_data_type_table_idx  group by (tp_decimal)  order by tp_decimal ",
    "select id_dt from full_data_type_table_idx  group by (id_dt)  order by id_dt ",
    "select tp_char from full_data_type_table_idx  group by (tp_char)  order by tp_char ",
    "select tp_date from full_data_type_table_idx  group by (tp_date)  order by tp_date "
  )

  allCases foreach { query =>
    {
      ignore(query) {
        runTest(query)
      }
    }
  }

}
