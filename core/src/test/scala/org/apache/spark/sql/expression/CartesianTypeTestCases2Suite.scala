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

import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.test.SharedSQLContext

class CartesianTypeTestCases2Suite extends BaseTiSparkSuite {
  private val allCases = Seq[String](
    "select tp_int,tp_decimal from full_data_type_table  where tp_int <> tp_decimal order by id_dt  limit 20",
    "select tp_mediumint,id_dt from full_data_type_table  where tp_mediumint <> id_dt order by id_dt  limit 20",
    "select tp_int,tp_smallint from full_data_type_table  where tp_int <> tp_smallint order by id_dt  limit 20",
    "select tp_char,tp_char from full_data_type_table  where tp_char <> tp_char order by id_dt  limit 20",
    "select tp_double,tp_tinyint from full_data_type_table  where tp_double <> tp_tinyint order by id_dt  limit 20",
    "select tp_float,id_dt from full_data_type_table  where tp_float <> id_dt order by id_dt  limit 20",
    "select tp_real,tp_smallint from full_data_type_table  where tp_real <> tp_smallint order by id_dt  limit 20",
    "select tp_tinytext,tp_text from full_data_type_table  where tp_tinytext <> tp_text order by id_dt  limit 20",
    "select id_dt,tp_tinyint from full_data_type_table  where id_dt <> tp_tinyint order by id_dt  limit 20",
    "select id_dt,tp_float from full_data_type_table  where id_dt <> tp_float order by id_dt  limit 20",
    "select tp_mediumint,tp_smallint from full_data_type_table  where tp_mediumint <> tp_smallint order by id_dt  limit 20",
    "select tp_decimal,id_dt from full_data_type_table  where tp_decimal <> id_dt order by id_dt  limit 20",
    "select tp_tinyint,tp_smallint from full_data_type_table  where tp_tinyint <> tp_smallint order by id_dt  limit 20",
    "select tp_double,tp_float from full_data_type_table  where tp_double <> tp_float order by id_dt  limit 20",
    "select tp_float,tp_mediumint from full_data_type_table  where tp_float <> tp_mediumint order by id_dt  limit 20",
    "select tp_mediumtext,tp_mediumtext from full_data_type_table  where tp_mediumtext <> tp_mediumtext order by id_dt  limit 20",
    "select tp_tinyint,id_dt from full_data_type_table  where tp_tinyint <> id_dt order by id_dt  limit 20",
    "select tp_decimal,tp_decimal from full_data_type_table  where tp_decimal <> tp_decimal order by id_dt  limit 20",
    "select tp_bigint,tp_real from full_data_type_table  where tp_bigint <> tp_real order by id_dt  limit 20",
    "select tp_datetime,tp_timestamp from full_data_type_table  where tp_datetime <> tp_timestamp order by id_dt  limit 20",
    "select tp_real,tp_bigint from full_data_type_table  where tp_real <> tp_bigint order by id_dt  limit 20",
    "select id_dt,tp_decimal from full_data_type_table  where id_dt <> tp_decimal order by id_dt  limit 20"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query, query.replace("full_data_type_table", "full_data_type_table_j"))
      }
    }
  }

}
