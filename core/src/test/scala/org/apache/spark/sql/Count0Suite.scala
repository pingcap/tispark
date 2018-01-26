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

class Count0Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select count(tp_int) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_int) from full_data_type_table ")
    val r2 = querySpark("select count(tp_int) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_date) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_date) from full_data_type_table ")
    val r2 = querySpark("select count(tp_date) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_tinytext) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_tinytext) from full_data_type_table ")
    val r2 = querySpark("select count(tp_tinytext) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_binary) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_binary) from full_data_type_table ")
    val r2 = querySpark("select count(tp_binary) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_decimal) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_decimal) from full_data_type_table ")
    val r2 = querySpark("select count(tp_decimal) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_timestamp) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_timestamp) from full_data_type_table ")
    val r2 = querySpark("select count(tp_timestamp) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_smallint) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_smallint) from full_data_type_table ")
    val r2 = querySpark("select count(tp_smallint) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_longtext) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_longtext) from full_data_type_table ")
    val r2 = querySpark("select count(tp_longtext) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_double) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_double) from full_data_type_table ")
    val r2 = querySpark("select count(tp_double) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_tinyint) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_tinyint) from full_data_type_table ")
    val r2 = querySpark("select count(tp_tinyint) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_mediumtext) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_mediumtext) from full_data_type_table ")
    val r2 = querySpark("select count(tp_mediumtext) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_varchar) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_varchar) from full_data_type_table ")
    val r2 = querySpark("select count(tp_varchar) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_text) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_text) from full_data_type_table ")
    val r2 = querySpark("select count(tp_text) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_float) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_float) from full_data_type_table ")
    val r2 = querySpark("select count(tp_float) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_datetime) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_datetime) from full_data_type_table ")
    val r2 = querySpark("select count(tp_datetime) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_mediumint) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_mediumint) from full_data_type_table ")
    val r2 = querySpark("select count(tp_mediumint) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_blob) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_blob) from full_data_type_table ")
    val r2 = querySpark("select count(tp_blob) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_char) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_char) from full_data_type_table ")
    val r2 = querySpark("select count(tp_char) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_nvarchar) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_nvarchar) from full_data_type_table ")
    val r2 = querySpark("select count(tp_nvarchar) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(id_dt) from full_data_type_table ") {
    val r1 = querySpark("select count(id_dt) from full_data_type_table ")
    val r2 = querySpark("select count(id_dt) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_real) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_real) from full_data_type_table ")
    val r2 = querySpark("select count(tp_real) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select count(tp_bigint) from full_data_type_table ") {
    val r1 = querySpark("select count(tp_bigint) from full_data_type_table ")
    val r2 = querySpark("select count(tp_bigint) from full_data_type_table_j ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}