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

class ArithmeticTest0Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select tp_tinyint + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal + 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal + 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal + 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal - 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal - 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal - 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_longtext * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_longtext * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_int * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_int * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_int * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinytext * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinytext * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinytext * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumtext * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumtext * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumtext * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_real * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_real * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_text * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_text * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal * 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_decimal * 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_decimal * 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_tinyint / 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_tinyint / 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_tinyint / 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float / 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_float / 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_float / 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select id_dt / 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select id_dt / 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select id_dt / 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint / 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_bigint / 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_bigint / 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 1.7976931348623157E308 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 1.7976931348623157E308 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double / 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_double / 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_double / 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 127 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 127 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 127 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / -128 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / -128 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / -128 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 0 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 0 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 0 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 2017 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 2017 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 2017 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_smallint / 2147868.65536 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_smallint / 2147868.65536 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_smallint / 2147868.65536 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint / 9223372036854775807 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint / 9223372036854775807 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint / -9223372036854775808 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint / -9223372036854775808 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint / 3.14159265358979 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint / 3.14159265358979 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint / 2.34E10 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint / 2.34E10 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint / 2.34E10 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint / 2147483647 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint / 2147483647 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint / 2147483647 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint / -2147483648 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint / -2147483648 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint / -2147483648 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint / 32767 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint / 32767 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint / 32767 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_mediumint / -32768 from full_data_type_table  order by id_dt  limit 10") {
    val r1 = querySpark("select tp_mediumint / -32768 from full_data_type_table  order by id_dt  limit 10")
    val r2 = querySpark("select tp_mediumint / -32768 from full_data_type_table_j  order by id_dt  limit 10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}