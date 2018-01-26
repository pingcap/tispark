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

class Aggregate0Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select tp_mediumtext from full_data_type_table  group by (tp_mediumtext)  order by tp_mediumtext ") {
    val r1 = querySpark("select tp_mediumtext from full_data_type_table  group by (tp_mediumtext)  order by tp_mediumtext ")
    val r2 = querySpark("select tp_mediumtext from full_data_type_table_j  group by (tp_mediumtext)  order by tp_mediumtext ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double from full_data_type_table  group by (tp_double)  order by tp_double ") {
    val r1 = querySpark("select tp_double from full_data_type_table  group by (tp_double)  order by tp_double ")
    val r2 = querySpark("select tp_double from full_data_type_table_j  group by (tp_double)  order by tp_double ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint from full_data_type_table  group by (tp_smallint)  order by tp_smallint ") {
    val r1 = querySpark("select tp_smallint from full_data_type_table  group by (tp_smallint)  order by tp_smallint ")
    val r2 = querySpark("select tp_smallint from full_data_type_table_j  group by (tp_smallint)  order by tp_smallint ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_nvarchar from full_data_type_table  group by (tp_nvarchar)  order by tp_nvarchar ") {
    val r1 = querySpark("select tp_nvarchar from full_data_type_table  group by (tp_nvarchar)  order by tp_nvarchar ")
    val r2 = querySpark("select tp_nvarchar from full_data_type_table_j  group by (tp_nvarchar)  order by tp_nvarchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real from full_data_type_table  group by (tp_real)  order by tp_real ") {
    val r1 = querySpark("select tp_real from full_data_type_table  group by (tp_real)  order by tp_real ")
    val r2 = querySpark("select tp_real from full_data_type_table_j  group by (tp_real)  order by tp_real ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_binary from full_data_type_table  group by (tp_binary)  order by tp_binary ") {
    val r1 = querySpark("select tp_binary from full_data_type_table  group by (tp_binary)  order by tp_binary ")
    val r2 = querySpark("select tp_binary from full_data_type_table_j  group by (tp_binary)  order by tp_binary ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text from full_data_type_table  group by (tp_text)  order by tp_text ") {
    val r1 = querySpark("select tp_text from full_data_type_table  group by (tp_text)  order by tp_text ")
    val r2 = querySpark("select tp_text from full_data_type_table_j  group by (tp_text)  order by tp_text ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_blob from full_data_type_table  group by (tp_blob)  order by tp_blob ") {
    val r1 = querySpark("select tp_blob from full_data_type_table  group by (tp_blob)  order by tp_blob ")
    val r2 = querySpark("select tp_blob from full_data_type_table_j  group by (tp_blob)  order by tp_blob ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_date from full_data_type_table  group by (tp_date)  order by tp_date ") {
    val r1 = querySpark("select tp_date from full_data_type_table  group by (tp_date)  order by tp_date ")
    val r2 = querySpark("select tp_date from full_data_type_table_j  group by (tp_date)  order by tp_date ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt from full_data_type_table  group by (id_dt)  order by id_dt ") {
    val r1 = querySpark("select id_dt from full_data_type_table  group by (id_dt)  order by id_dt ")
    val r2 = querySpark("select id_dt from full_data_type_table_j  group by (id_dt)  order by id_dt ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint from full_data_type_table  group by (tp_mediumint)  order by tp_mediumint ") {
    val r1 = querySpark("select tp_mediumint from full_data_type_table  group by (tp_mediumint)  order by tp_mediumint ")
    val r2 = querySpark("select tp_mediumint from full_data_type_table_j  group by (tp_mediumint)  order by tp_mediumint ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint from full_data_type_table  group by (tp_tinyint)  order by tp_tinyint ") {
    val r1 = querySpark("select tp_tinyint from full_data_type_table  group by (tp_tinyint)  order by tp_tinyint ")
    val r2 = querySpark("select tp_tinyint from full_data_type_table_j  group by (tp_tinyint)  order by tp_tinyint ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext from full_data_type_table  group by (tp_tinytext)  order by tp_tinytext ") {
    val r1 = querySpark("select tp_tinytext from full_data_type_table  group by (tp_tinytext)  order by tp_tinytext ")
    val r2 = querySpark("select tp_tinytext from full_data_type_table_j  group by (tp_tinytext)  order by tp_tinytext ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float from full_data_type_table  group by (tp_float)  order by tp_float ") {
    val r1 = querySpark("select tp_float from full_data_type_table  group by (tp_float)  order by tp_float ")
    val r2 = querySpark("select tp_float from full_data_type_table_j  group by (tp_float)  order by tp_float ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint from full_data_type_table  group by (tp_bigint)  order by tp_bigint ") {
    val r1 = querySpark("select tp_bigint from full_data_type_table  group by (tp_bigint)  order by tp_bigint ")
    val r2 = querySpark("select tp_bigint from full_data_type_table_j  group by (tp_bigint)  order by tp_bigint ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int from full_data_type_table  group by (tp_int)  order by tp_int ") {
    val r1 = querySpark("select tp_int from full_data_type_table  group by (tp_int)  order by tp_int ")
    val r2 = querySpark("select tp_int from full_data_type_table_j  group by (tp_int)  order by tp_int ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_timestamp from full_data_type_table  group by (tp_timestamp)  order by tp_timestamp ") {
    val r1 = querySpark("select tp_timestamp from full_data_type_table  group by (tp_timestamp)  order by tp_timestamp ")
    val r2 = querySpark("select tp_timestamp from full_data_type_table_j  group by (tp_timestamp)  order by tp_timestamp ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal from full_data_type_table  group by (tp_decimal)  order by tp_decimal ") {
    val r1 = querySpark("select tp_decimal from full_data_type_table  group by (tp_decimal)  order by tp_decimal ")
    val r2 = querySpark("select tp_decimal from full_data_type_table_j  group by (tp_decimal)  order by tp_decimal ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_char from full_data_type_table  group by (tp_char)  order by tp_char ") {
    val r1 = querySpark("select tp_char from full_data_type_table  group by (tp_char)  order by tp_char ")
    val r2 = querySpark("select tp_char from full_data_type_table_j  group by (tp_char)  order by tp_char ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext from full_data_type_table  group by (tp_longtext)  order by tp_longtext ") {
    val r1 = querySpark("select tp_longtext from full_data_type_table  group by (tp_longtext)  order by tp_longtext ")
    val r2 = querySpark("select tp_longtext from full_data_type_table_j  group by (tp_longtext)  order by tp_longtext ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_varchar from full_data_type_table  group by (tp_varchar)  order by tp_varchar ") {
    val r1 = querySpark("select tp_varchar from full_data_type_table  group by (tp_varchar)  order by tp_varchar ")
    val r2 = querySpark("select tp_varchar from full_data_type_table_j  group by (tp_varchar)  order by tp_varchar ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_datetime from full_data_type_table  group by (tp_datetime)  order by tp_datetime ") {
    val r1 = querySpark("select tp_datetime from full_data_type_table  group by (tp_datetime)  order by tp_datetime ")
    val r2 = querySpark("select tp_datetime from full_data_type_table_j  group by (tp_datetime)  order by tp_datetime ")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}