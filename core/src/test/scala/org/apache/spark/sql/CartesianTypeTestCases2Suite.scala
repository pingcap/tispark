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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

class CartesianTypeTestCases2Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select tp_int,tp_decimal from full_data_type_table  where tp_int <> tp_decimal order by id_dt  limit 20") {
    val r1 = querySpark("select tp_int,tp_decimal from full_data_type_table  where tp_int <> tp_decimal order by id_dt  limit 20")
    val r2 = querySpark("select tp_int,tp_decimal from full_data_type_table_j  where tp_int <> tp_decimal order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint,id_dt from full_data_type_table  where tp_mediumint <> id_dt order by id_dt  limit 20") {
    val r1 = querySpark("select tp_mediumint,id_dt from full_data_type_table  where tp_mediumint <> id_dt order by id_dt  limit 20")
    val r2 = querySpark("select tp_mediumint,id_dt from full_data_type_table_j  where tp_mediumint <> id_dt order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int,tp_smallint from full_data_type_table  where tp_int <> tp_smallint order by id_dt  limit 20") {
    val r1 = querySpark("select tp_int,tp_smallint from full_data_type_table  where tp_int <> tp_smallint order by id_dt  limit 20")
    val r2 = querySpark("select tp_int,tp_smallint from full_data_type_table_j  where tp_int <> tp_smallint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_char,tp_char from full_data_type_table  where tp_char <> tp_char order by id_dt  limit 20") {
    val r1 = querySpark("select tp_char,tp_char from full_data_type_table  where tp_char <> tp_char order by id_dt  limit 20")
    val r2 = querySpark("select tp_char,tp_char from full_data_type_table_j  where tp_char <> tp_char order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double,tp_tinyint from full_data_type_table  where tp_double <> tp_tinyint order by id_dt  limit 20") {
    val r1 = querySpark("select tp_double,tp_tinyint from full_data_type_table  where tp_double <> tp_tinyint order by id_dt  limit 20")
    val r2 = querySpark("select tp_double,tp_tinyint from full_data_type_table_j  where tp_double <> tp_tinyint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float,id_dt from full_data_type_table  where tp_float <> id_dt order by id_dt  limit 20") {
    val r1 = querySpark("select tp_float,id_dt from full_data_type_table  where tp_float <> id_dt order by id_dt  limit 20")
    val r2 = querySpark("select tp_float,id_dt from full_data_type_table_j  where tp_float <> id_dt order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real,tp_smallint from full_data_type_table  where tp_real <> tp_smallint order by id_dt  limit 20") {
    val r1 = querySpark("select tp_real,tp_smallint from full_data_type_table  where tp_real <> tp_smallint order by id_dt  limit 20")
    val r2 = querySpark("select tp_real,tp_smallint from full_data_type_table_j  where tp_real <> tp_smallint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext,tp_text from full_data_type_table  where tp_tinytext <> tp_text order by id_dt  limit 20") {
    val r1 = querySpark("select tp_tinytext,tp_text from full_data_type_table  where tp_tinytext <> tp_text order by id_dt  limit 20")
    val r2 = querySpark("select tp_tinytext,tp_text from full_data_type_table_j  where tp_tinytext <> tp_text order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt,tp_tinyint from full_data_type_table  where id_dt <> tp_tinyint order by id_dt  limit 20") {
    val r1 = querySpark("select id_dt,tp_tinyint from full_data_type_table  where id_dt <> tp_tinyint order by id_dt  limit 20")
    val r2 = querySpark("select id_dt,tp_tinyint from full_data_type_table_j  where id_dt <> tp_tinyint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt,tp_float from full_data_type_table  where id_dt <> tp_float order by id_dt  limit 20") {
    val r1 = querySpark("select id_dt,tp_float from full_data_type_table  where id_dt <> tp_float order by id_dt  limit 20")
    val r2 = querySpark("select id_dt,tp_float from full_data_type_table_j  where id_dt <> tp_float order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text,tp_bigint from full_data_type_table  where tp_text <> tp_bigint order by id_dt  limit 20") {
    val r1 = querySpark("select tp_text,tp_bigint from full_data_type_table  where tp_text <> tp_bigint order by id_dt  limit 20")
    val r2 = querySpark("select tp_text,tp_bigint from full_data_type_table_j  where tp_text <> tp_bigint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint,tp_smallint from full_data_type_table  where tp_mediumint <> tp_smallint order by id_dt  limit 20") {
    val r1 = querySpark("select tp_mediumint,tp_smallint from full_data_type_table  where tp_mediumint <> tp_smallint order by id_dt  limit 20")
    val r2 = querySpark("select tp_mediumint,tp_smallint from full_data_type_table_j  where tp_mediumint <> tp_smallint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt,tp_tinytext from full_data_type_table  where id_dt <> tp_tinytext order by id_dt  limit 20") {
    val r1 = querySpark("select id_dt,tp_tinytext from full_data_type_table  where id_dt <> tp_tinytext order by id_dt  limit 20")
    val r2 = querySpark("select id_dt,tp_tinytext from full_data_type_table_j  where id_dt <> tp_tinytext order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal,id_dt from full_data_type_table  where tp_decimal <> id_dt order by id_dt  limit 20") {
    val r1 = querySpark("select tp_decimal,id_dt from full_data_type_table  where tp_decimal <> id_dt order by id_dt  limit 20")
    val r2 = querySpark("select tp_decimal,id_dt from full_data_type_table_j  where tp_decimal <> id_dt order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_datetime,tp_text from full_data_type_table  where tp_datetime <> tp_text order by id_dt  limit 20") {
    val r1 = querySpark("select tp_datetime,tp_text from full_data_type_table  where tp_datetime <> tp_text order by id_dt  limit 20")
    val r2 = querySpark("select tp_datetime,tp_text from full_data_type_table_j  where tp_datetime <> tp_text order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint,tp_smallint from full_data_type_table  where tp_tinyint <> tp_smallint order by id_dt  limit 20") {
    val r1 = querySpark("select tp_tinyint,tp_smallint from full_data_type_table  where tp_tinyint <> tp_smallint order by id_dt  limit 20")
    val r2 = querySpark("select tp_tinyint,tp_smallint from full_data_type_table_j  where tp_tinyint <> tp_smallint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text,tp_int from full_data_type_table  where tp_text <> tp_int order by id_dt  limit 20") {
    val r1 = querySpark("select tp_text,tp_int from full_data_type_table  where tp_text <> tp_int order by id_dt  limit 20")
    val r2 = querySpark("select tp_text,tp_int from full_data_type_table_j  where tp_text <> tp_int order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal,tp_tinytext from full_data_type_table  where tp_decimal <> tp_tinytext order by id_dt  limit 20") {
    val r1 = querySpark("select tp_decimal,tp_tinytext from full_data_type_table  where tp_decimal <> tp_tinytext order by id_dt  limit 20")
    val r2 = querySpark("select tp_decimal,tp_tinytext from full_data_type_table_j  where tp_decimal <> tp_tinytext order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text,id_dt from full_data_type_table  where tp_text <> id_dt order by id_dt  limit 20") {
    val r1 = querySpark("select tp_text,id_dt from full_data_type_table  where tp_text <> id_dt order by id_dt  limit 20")
    val r2 = querySpark("select tp_text,id_dt from full_data_type_table_j  where tp_text <> id_dt order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double,tp_float from full_data_type_table  where tp_double <> tp_float order by id_dt  limit 20") {
    val r1 = querySpark("select tp_double,tp_float from full_data_type_table  where tp_double <> tp_float order by id_dt  limit 20")
    val r2 = querySpark("select tp_double,tp_float from full_data_type_table_j  where tp_double <> tp_float order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float,tp_mediumint from full_data_type_table  where tp_float <> tp_mediumint order by id_dt  limit 20") {
    val r1 = querySpark("select tp_float,tp_mediumint from full_data_type_table  where tp_float <> tp_mediumint order by id_dt  limit 20")
    val r2 = querySpark("select tp_float,tp_mediumint from full_data_type_table_j  where tp_float <> tp_mediumint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext,tp_datetime from full_data_type_table  where tp_tinytext <> tp_datetime order by id_dt  limit 20") {
    val r1 = querySpark("select tp_tinytext,tp_datetime from full_data_type_table  where tp_tinytext <> tp_datetime order by id_dt  limit 20")
    val r2 = querySpark("select tp_tinytext,tp_datetime from full_data_type_table_j  where tp_tinytext <> tp_datetime order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext,tp_mediumtext from full_data_type_table  where tp_mediumtext <> tp_mediumtext order by id_dt  limit 20") {
    val r1 = querySpark("select tp_mediumtext,tp_mediumtext from full_data_type_table  where tp_mediumtext <> tp_mediumtext order by id_dt  limit 20")
    val r2 = querySpark("select tp_mediumtext,tp_mediumtext from full_data_type_table_j  where tp_mediumtext <> tp_mediumtext order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint,id_dt from full_data_type_table  where tp_tinyint <> id_dt order by id_dt  limit 20") {
    val r1 = querySpark("select tp_tinyint,id_dt from full_data_type_table  where tp_tinyint <> id_dt order by id_dt  limit 20")
    val r2 = querySpark("select tp_tinyint,id_dt from full_data_type_table_j  where tp_tinyint <> id_dt order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal,tp_decimal from full_data_type_table  where tp_decimal <> tp_decimal order by id_dt  limit 20") {
    val r1 = querySpark("select tp_decimal,tp_decimal from full_data_type_table  where tp_decimal <> tp_decimal order by id_dt  limit 20")
    val r2 = querySpark("select tp_decimal,tp_decimal from full_data_type_table_j  where tp_decimal <> tp_decimal order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint,tp_real from full_data_type_table  where tp_bigint <> tp_real order by id_dt  limit 20") {
    val r1 = querySpark("select tp_bigint,tp_real from full_data_type_table  where tp_bigint <> tp_real order by id_dt  limit 20")
    val r2 = querySpark("select tp_bigint,tp_real from full_data_type_table_j  where tp_bigint <> tp_real order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_datetime,tp_timestamp from full_data_type_table  where tp_datetime <> tp_timestamp order by id_dt  limit 20") {
    val r1 = querySpark("select tp_datetime,tp_timestamp from full_data_type_table  where tp_datetime <> tp_timestamp order by id_dt  limit 20")
    val r2 = querySpark("select tp_datetime,tp_timestamp from full_data_type_table_j  where tp_datetime <> tp_timestamp order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real,tp_bigint from full_data_type_table  where tp_real <> tp_bigint order by id_dt  limit 20") {
    val r1 = querySpark("select tp_real,tp_bigint from full_data_type_table  where tp_real <> tp_bigint order by id_dt  limit 20")
    val r2 = querySpark("select tp_real,tp_bigint from full_data_type_table_j  where tp_real <> tp_bigint order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt,tp_decimal from full_data_type_table  where id_dt <> tp_decimal order by id_dt  limit 20") {
    val r1 = querySpark("select id_dt,tp_decimal from full_data_type_table  where id_dt <> tp_decimal order by id_dt  limit 20")
    val r2 = querySpark("select id_dt,tp_decimal from full_data_type_table_j  where id_dt <> tp_decimal order by id_dt  limit 20")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}