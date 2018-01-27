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

class PlaceHolderTest1Suite extends BaseTiSparkSuite with SharedSQLContext {

  test("select  count(1)  from full_data_type_table  where tp_float > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > null",
      "select  count(1)  from full_data_type_table_j  where tp_float > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_float > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_float > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_float > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_float > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_float > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_float > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_float > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_float > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_float > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_float > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_float > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_float > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_float > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_float > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 127",
      "select  count(1)  from full_data_type_table_j  where tp_float > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > -128",
      "select  count(1)  from full_data_type_table_j  where tp_float > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 0",
      "select  count(1)  from full_data_type_table_j  where tp_float > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_float > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_float > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_blob > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_blob > null",
      "select  count(1)  from full_data_type_table_j  where tp_blob > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > null",
      "select  count(1)  from full_data_type_table_j  where id_dt > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where id_dt > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where id_dt > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where id_dt > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where id_dt > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where id_dt > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where id_dt > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where id_dt > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where id_dt > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where id_dt > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where id_dt > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 2147483647",
      "select  count(1)  from full_data_type_table_j  where id_dt > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > -2147483648",
      "select  count(1)  from full_data_type_table_j  where id_dt > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 32767",
      "select  count(1)  from full_data_type_table_j  where id_dt > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > -32768",
      "select  count(1)  from full_data_type_table_j  where id_dt > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 127",
      "select  count(1)  from full_data_type_table_j  where id_dt > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > -128",
      "select  count(1)  from full_data_type_table_j  where id_dt > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 0",
      "select  count(1)  from full_data_type_table_j  where id_dt > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 2017",
      "select  count(1)  from full_data_type_table_j  where id_dt > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where id_dt > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > null",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 127",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > -128",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 0",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_bigint > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > null",
      "select  count(1)  from full_data_type_table_j  where tp_double > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_double > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_double > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_double > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_double > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_double > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_double > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_double > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_double > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_double > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_double > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_double > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_double > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_double > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_double > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 127",
      "select  count(1)  from full_data_type_table_j  where tp_double > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > -128",
      "select  count(1)  from full_data_type_table_j  where tp_double > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 0",
      "select  count(1)  from full_data_type_table_j  where tp_double > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_double > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_double > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > null",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 127",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > -128",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 0",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime > null",
      "select  count(1)  from full_data_type_table_j  where tp_datetime > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime > 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime > 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime > 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > null",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 127",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > -128",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 0",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_smallint > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date > null",
      "select  count(1)  from full_data_type_table_j  where tp_date > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date > 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date > 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_date > 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_date > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_date > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_date > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_date > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_date > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > null",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 127",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > -128",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 0",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_varchar > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > null",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 127",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > -128",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 0",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_longtext > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_longtext > null",
      "select  count(1)  from full_data_type_table_j  where tp_longtext > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > null",
      "select  count(1)  from full_data_type_table_j  where tp_int > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_int > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_int > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_int > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_int > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_int > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_int > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_int > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_int > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_int > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_int > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_int > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_int > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_int > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_int > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 127",
      "select  count(1)  from full_data_type_table_j  where tp_int > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > -128",
      "select  count(1)  from full_data_type_table_j  where tp_int > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 0",
      "select  count(1)  from full_data_type_table_j  where tp_int > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_int > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_int > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinytext > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinytext > null",
      "select  count(1)  from full_data_type_table_j  where tp_tinytext > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp > null",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp > 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp > 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp > 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumtext > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumtext > null",
      "select  count(1)  from full_data_type_table_j  where tp_mediumtext > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > null",
      "select  count(1)  from full_data_type_table_j  where tp_real > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_real > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_real > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_real > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_real > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_real > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_real > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_real > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_real > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_real > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_real > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_real > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_real > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_real > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_real > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 127",
      "select  count(1)  from full_data_type_table_j  where tp_real > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > -128",
      "select  count(1)  from full_data_type_table_j  where tp_real > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 0",
      "select  count(1)  from full_data_type_table_j  where tp_real > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_real > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_real > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_text > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_text > null",
      "select  count(1)  from full_data_type_table_j  where tp_text > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > null",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 32767",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > -32768",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 127",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > -128",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 0",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 2017",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal > 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal > 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_decimal > 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_binary > null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_binary > null",
      "select  count(1)  from full_data_type_table_j  where tp_binary > null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= null",
      "select  count(1)  from full_data_type_table_j  where tp_char <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_char <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_char <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_char <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_char <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_char <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_char <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_char <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_char <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_char <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= null",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= null",
      "select  count(1)  from full_data_type_table_j  where tp_float <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_float <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_float <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_float <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_float <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_float <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_float <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_float <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_float <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_float <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_blob <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_blob <= null",
      "select  count(1)  from full_data_type_table_j  where tp_blob <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= null",
      "select  count(1)  from full_data_type_table_j  where id_dt <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where id_dt <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where id_dt <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where id_dt <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where id_dt <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where id_dt <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where id_dt <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 32767",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= -32768",
      "select  count(1)  from full_data_type_table_j  where id_dt <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 127",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= -128",
      "select  count(1)  from full_data_type_table_j  where id_dt <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 0",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 2017",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where id_dt <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= null",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_bigint <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_bigint <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_bigint <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= null",
      "select  count(1)  from full_data_type_table_j  where tp_double <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_double <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_double <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_double <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_double <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_double <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_double <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_double <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_double <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_double <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_double <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_double <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= null",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_nvarchar <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_nvarchar <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime <= null",
      "select  count(1)  from full_data_type_table_j  where tp_datetime <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_datetime <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_datetime <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_datetime <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= null",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_smallint <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_smallint <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_smallint <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date <= null",
      "select  count(1)  from full_data_type_table_j  where tp_date <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_date <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_date <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_date <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_date <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_date <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_date <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_date <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_date <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= null",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_varchar <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_varchar <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_varchar <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= null",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumint <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumint <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_mediumint <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_longtext <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_longtext <= null",
      "select  count(1)  from full_data_type_table_j  where tp_longtext <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= null",
      "select  count(1)  from full_data_type_table_j  where tp_int <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_int <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_int <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_int <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_int <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_int <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_int <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_int <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_int <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_int <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_int <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_int <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinytext <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinytext <= null",
      "select  count(1)  from full_data_type_table_j  where tp_tinytext <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp <= null",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp <= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp <= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp <= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_timestamp <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_timestamp <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_timestamp <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_mediumtext <= null",
      "select  count(1)  from full_data_type_table_j  where tp_mediumtext <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= null",
      "select  count(1)  from full_data_type_table_j  where tp_real <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_real <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_real <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_real <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_real <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_real <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_real <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_real <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_real <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_real <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_real <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_real <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_text <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_text <= null",
      "select  count(1)  from full_data_type_table_j  where tp_text <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= null",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 127",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= -128",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 0",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_decimal <= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_decimal <= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_decimal <= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_binary <= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_binary <= null",
      "select  count(1)  from full_data_type_table_j  where tp_binary <= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= null",
      "select  count(1)  from full_data_type_table_j  where tp_char >= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_char >= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_char >= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_char >= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_char >= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_char >= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_char >= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_char >= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 127",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= -128",
      "select  count(1)  from full_data_type_table_j  where tp_char >= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 0",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_char >= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_char >= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_char >= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= null",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 'PingCAP'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 'PingCAP'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 'PingCAP'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 127",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= -128",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 0",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_tinyint >= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_tinyint >= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_tinyint >= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= null",
      "select  count(1)  from full_data_type_table_j  where tp_float >= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= '2017-11-02'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= '2017-11-02'",
      "select  count(1)  from full_data_type_table_j  where tp_float >= '2017-11-02'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= '2017-10-30'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= '2017-10-30'",
      "select  count(1)  from full_data_type_table_j  where tp_float >= '2017-10-30'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= '2017-09-07 11:11:11'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= '2017-09-07 11:11:11'",
      "select  count(1)  from full_data_type_table_j  where tp_float >= '2017-09-07 11:11:11'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= '2017-11-02 08:47:43'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= '2017-11-02 08:47:43'",
      "select  count(1)  from full_data_type_table_j  where tp_float >= '2017-11-02 08:47:43'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 'fYfSp'") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 'fYfSp'",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 'fYfSp'"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 9223372036854775807") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 9223372036854775807",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 9223372036854775807"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= -9223372036854775808") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= -9223372036854775808",
      "select  count(1)  from full_data_type_table_j  where tp_float >= -9223372036854775808"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 1.7976931348623157E308") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 1.7976931348623157E308",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 1.7976931348623157E308"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 3.14159265358979") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 3.14159265358979",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 3.14159265358979"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 2.34E10") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 2.34E10",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 2.34E10"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 2147483647") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 2147483647",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 2147483647"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= -2147483648") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= -2147483648",
      "select  count(1)  from full_data_type_table_j  where tp_float >= -2147483648"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 32767") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 32767",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 32767"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= -32768") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= -32768",
      "select  count(1)  from full_data_type_table_j  where tp_float >= -32768"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 127") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 127",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 127"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= -128") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= -128",
      "select  count(1)  from full_data_type_table_j  where tp_float >= -128"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 0") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 0",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 0"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 2017") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 2017",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 2017"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_float >= 2147868.65536") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_float >= 2147868.65536",
      "select  count(1)  from full_data_type_table_j  where tp_float >= 2147868.65536"
    )
  }

  test("select  count(1)  from full_data_type_table  where tp_blob >= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where tp_blob >= null",
      "select  count(1)  from full_data_type_table_j  where tp_blob >= null"
    )
  }

  test("select  count(1)  from full_data_type_table  where id_dt >= null") {
    runTest(
      "select  count(1)  from full_data_type_table  where id_dt >= null",
      "select  count(1)  from full_data_type_table_j  where id_dt >= null"
    )
  }

}
