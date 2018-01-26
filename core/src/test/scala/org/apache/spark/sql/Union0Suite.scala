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

class Union0Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("(select tp_decimal from full_data_type_table where tp_decimal < 0) union (select tp_decimal from full_data_type_table where tp_decimal > 0) order by tp_decimal") {
    val r1 = querySpark("(select tp_decimal from full_data_type_table where tp_decimal < 0) union (select tp_decimal from full_data_type_table where tp_decimal > 0) order by tp_decimal")
    val r2 = querySpark("(select tp_decimal from full_data_type_table_j where tp_decimal < 0) union (select tp_decimal from full_data_type_table_j where tp_decimal > 0) order by tp_decimal")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_text from full_data_type_table where tp_text < 0) union (select tp_text from full_data_type_table where tp_text > 0) order by tp_text") {
    val r1 = querySpark("(select tp_text from full_data_type_table where tp_text < 0) union (select tp_text from full_data_type_table where tp_text > 0) order by tp_text")
    val r2 = querySpark("(select tp_text from full_data_type_table_j where tp_text < 0) union (select tp_text from full_data_type_table_j where tp_text > 0) order by tp_text")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_smallint from full_data_type_table where tp_smallint < 0) union (select tp_smallint from full_data_type_table where tp_smallint > 0) order by tp_smallint") {
    val r1 = querySpark("(select tp_smallint from full_data_type_table where tp_smallint < 0) union (select tp_smallint from full_data_type_table where tp_smallint > 0) order by tp_smallint")
    val r2 = querySpark("(select tp_smallint from full_data_type_table_j where tp_smallint < 0) union (select tp_smallint from full_data_type_table_j where tp_smallint > 0) order by tp_smallint")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select id_dt from full_data_type_table where id_dt < 0) union (select id_dt from full_data_type_table where id_dt > 0) order by id_dt") {
    val r1 = querySpark("(select id_dt from full_data_type_table where id_dt < 0) union (select id_dt from full_data_type_table where id_dt > 0) order by id_dt")
    val r2 = querySpark("(select id_dt from full_data_type_table_j where id_dt < 0) union (select id_dt from full_data_type_table_j where id_dt > 0) order by id_dt")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_int from full_data_type_table where tp_int < 0) union (select tp_int from full_data_type_table where tp_int > 0) order by tp_int") {
    val r1 = querySpark("(select tp_int from full_data_type_table where tp_int < 0) union (select tp_int from full_data_type_table where tp_int > 0) order by tp_int")
    val r2 = querySpark("(select tp_int from full_data_type_table_j where tp_int < 0) union (select tp_int from full_data_type_table_j where tp_int > 0) order by tp_int")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_tinytext from full_data_type_table where tp_tinytext < 0) union (select tp_tinytext from full_data_type_table where tp_tinytext > 0) order by tp_tinytext") {
    val r1 = querySpark("(select tp_tinytext from full_data_type_table where tp_tinytext < 0) union (select tp_tinytext from full_data_type_table where tp_tinytext > 0) order by tp_tinytext")
    val r2 = querySpark("(select tp_tinytext from full_data_type_table_j where tp_tinytext < 0) union (select tp_tinytext from full_data_type_table_j where tp_tinytext > 0) order by tp_tinytext")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_bigint from full_data_type_table where tp_bigint < 0) union (select tp_bigint from full_data_type_table where tp_bigint > 0) order by tp_bigint") {
    val r1 = querySpark("(select tp_bigint from full_data_type_table where tp_bigint < 0) union (select tp_bigint from full_data_type_table where tp_bigint > 0) order by tp_bigint")
    val r2 = querySpark("(select tp_bigint from full_data_type_table_j where tp_bigint < 0) union (select tp_bigint from full_data_type_table_j where tp_bigint > 0) order by tp_bigint")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_mediumint from full_data_type_table where tp_mediumint < 0) union (select tp_mediumint from full_data_type_table where tp_mediumint > 0) order by tp_mediumint") {
    val r1 = querySpark("(select tp_mediumint from full_data_type_table where tp_mediumint < 0) union (select tp_mediumint from full_data_type_table where tp_mediumint > 0) order by tp_mediumint")
    val r2 = querySpark("(select tp_mediumint from full_data_type_table_j where tp_mediumint < 0) union (select tp_mediumint from full_data_type_table_j where tp_mediumint > 0) order by tp_mediumint")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_real from full_data_type_table where tp_real < 0) union (select tp_real from full_data_type_table where tp_real > 0) order by tp_real") {
    val r1 = querySpark("(select tp_real from full_data_type_table where tp_real < 0) union (select tp_real from full_data_type_table where tp_real > 0) order by tp_real")
    val r2 = querySpark("(select tp_real from full_data_type_table_j where tp_real < 0) union (select tp_real from full_data_type_table_j where tp_real > 0) order by tp_real")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_float from full_data_type_table where tp_float < 0) union (select tp_float from full_data_type_table where tp_float > 0) order by tp_float") {
    val r1 = querySpark("(select tp_float from full_data_type_table where tp_float < 0) union (select tp_float from full_data_type_table where tp_float > 0) order by tp_float")
    val r2 = querySpark("(select tp_float from full_data_type_table_j where tp_float < 0) union (select tp_float from full_data_type_table_j where tp_float > 0) order by tp_float")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_double from full_data_type_table where tp_double < 0) union (select tp_double from full_data_type_table where tp_double > 0) order by tp_double") {
    val r1 = querySpark("(select tp_double from full_data_type_table where tp_double < 0) union (select tp_double from full_data_type_table where tp_double > 0) order by tp_double")
    val r2 = querySpark("(select tp_double from full_data_type_table_j where tp_double < 0) union (select tp_double from full_data_type_table_j where tp_double > 0) order by tp_double")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("(select tp_tinyint from full_data_type_table where tp_tinyint < 0) union (select tp_tinyint from full_data_type_table where tp_tinyint > 0) order by tp_tinyint") {
    val r1 = querySpark("(select tp_tinyint from full_data_type_table where tp_tinyint < 0) union (select tp_tinyint from full_data_type_table where tp_tinyint > 0) order by tp_tinyint")
    val r2 = querySpark("(select tp_tinyint from full_data_type_table_j where tp_tinyint < 0) union (select tp_tinyint from full_data_type_table_j where tp_tinyint > 0) order by tp_tinyint")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}