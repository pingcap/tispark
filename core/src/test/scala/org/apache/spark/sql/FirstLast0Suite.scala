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

class FirstLast0Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select first(tp_char) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_char) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_char) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_double) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_double) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_double) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_double) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_double) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_double) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_decimal) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_decimal) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_decimal) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_bigint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_bigint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_bigint) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_bigint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_bigint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_bigint) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_date) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_date) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_date) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_varchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_varchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_varchar) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_tinytext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_tinytext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_tinytext) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_float) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_float) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_float) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_mediumtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_mediumtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_mediumtext) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_int) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_int) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_int) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(id_dt) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(id_dt) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(id_dt) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_real) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_real) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_real) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_datetime) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_datetime) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_datetime) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_smallint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_smallint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_smallint) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_tinyint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_tinyint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_tinyint) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_tinytext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_tinytext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_tinytext) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_longtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_longtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_longtext) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_blob) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_blob) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_blob) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_text) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_text) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_text) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_date) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_date) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_date) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_mediumint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_mediumint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_mediumint) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_datetime) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_datetime) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_datetime) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_smallint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_smallint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_smallint) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_mediumint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_mediumint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_mediumint) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_longtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_longtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_longtext) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_float) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_float) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_float) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_binary) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_binary) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_binary) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_binary) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_binary) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_binary) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_blob) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_blob) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_blob) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_nvarchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_nvarchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_nvarchar) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_text) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_text) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_text) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_char) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_char) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_char) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(id_dt) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(id_dt) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(id_dt) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_decimal) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_decimal) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_decimal) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_varchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_varchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_varchar) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_nvarchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_nvarchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_nvarchar) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_timestamp) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_timestamp) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_timestamp) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_timestamp) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_timestamp) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_timestamp) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_tinyint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_tinyint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_tinyint) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_int) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_int) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_int) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select first(tp_real) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select first(tp_real) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select first(tp_real) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select last(tp_mediumtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ") {
    val r1 = querySpark("select last(tp_mediumtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")
    val r2 = querySpark("select last(tp_mediumtext) from full_data_type_table_j  group by (tp_nvarchar)   order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}