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

import org.apache.spark.sql.BaseInitialOnceTest

class SimpleSelect0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select tp_text from full_data_type_table  order by tp_text  limit 20",
    "select tp_binary from full_data_type_table  order by tp_binary  limit 20",
    "select tp_float from full_data_type_table  order by tp_float  limit 20",
    "select tp_longtext from full_data_type_table  order by tp_longtext  limit 20",
    "select tp_bigint from full_data_type_table  order by tp_bigint  limit 20",
    "select tp_timestamp from full_data_type_table  order by tp_timestamp  limit 20",
    "select tp_tinyint from full_data_type_table  order by tp_tinyint  limit 20",
    "select tp_char from full_data_type_table  order by tp_char  limit 20",
    "select tp_decimal from full_data_type_table  order by tp_decimal  limit 20",
    "select tp_date from full_data_type_table  order by tp_date  limit 20",
    "select tp_mediumint from full_data_type_table  order by tp_mediumint  limit 20",
    "select tp_datetime from full_data_type_table  order by tp_datetime  limit 20",
    "select tp_mediumtext from full_data_type_table  order by tp_mediumtext  limit 20",
    "select tp_int from full_data_type_table  order by tp_int  limit 20",
    "select tp_double from full_data_type_table  order by tp_double  limit 20",
    "select tp_real from full_data_type_table  order by tp_real  limit 20",
    "select tp_smallint from full_data_type_table  order by tp_smallint  limit 20",
    "select tp_nvarchar from full_data_type_table  order by tp_nvarchar  limit 20",
    "select id_dt from full_data_type_table  order by id_dt  limit 20",
    "select tp_blob from full_data_type_table  order by tp_blob  limit 20",
    "select tp_varchar from full_data_type_table  order by tp_varchar  limit 20",
    "select tp_tinytext from full_data_type_table  order by tp_tinytext  limit 20"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
