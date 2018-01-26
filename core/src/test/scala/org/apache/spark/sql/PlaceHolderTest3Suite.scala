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

class PlaceHolderTest3Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select  count(1)  from full_data_type_table  where tp_double <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_double <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_double <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_double <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_nvarchar <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_nvarchar <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_nvarchar <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_datetime <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_datetime <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_datetime <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_datetime <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_datetime <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_datetime <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_datetime <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_datetime <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_datetime <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_datetime <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_datetime <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_datetime <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_datetime <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_datetime <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_datetime <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_datetime <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_datetime <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_datetime <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_datetime <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_datetime <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_datetime <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_smallint <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_smallint <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_smallint <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_date <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_date <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_date <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_date <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_date <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_date <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_date <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_date <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_date <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_date <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_date <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_date <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_date <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_date <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_date <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_date <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_date <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_date <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_date <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_date <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_date <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_varchar <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_varchar <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_varchar <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumint <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumint <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumint <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_longtext <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_longtext <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_longtext <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_int <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_int <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_int <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_tinytext <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_tinytext <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_tinytext <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_timestamp <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_timestamp <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_timestamp <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_timestamp <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_timestamp <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_timestamp <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_timestamp <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_timestamp <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_timestamp <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_timestamp <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_timestamp <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_timestamp <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_timestamp <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_mediumtext <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_mediumtext <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_mediumtext <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_real <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_real <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_real <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 'PingCAP'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 'PingCAP'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 'PingCAP'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_text <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_text <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_text <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> '2017-11-02'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> '2017-11-02'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> '2017-11-02'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> '2017-10-30'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> '2017-10-30'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> '2017-10-30'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> '2017-09-07 11:11:11'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> '2017-09-07 11:11:11'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> '2017-09-07 11:11:11'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> '2017-11-02 08:47:43'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> '2017-11-02 08:47:43'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> '2017-11-02 08:47:43'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 'fYfSp'") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 'fYfSp'")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 'fYfSp'")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 9223372036854775807") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 9223372036854775807")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 9223372036854775807")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> -9223372036854775808") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> -9223372036854775808")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> -9223372036854775808")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 1.7976931348623157E308") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 1.7976931348623157E308")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 1.7976931348623157E308")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 3.14159265358979") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 3.14159265358979")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 3.14159265358979")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 2.34E10") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 2.34E10")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 2.34E10")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 2147483647") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 2147483647")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 2147483647")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> -2147483648") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> -2147483648")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> -2147483648")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 32767") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 32767")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 32767")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> -32768") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> -32768")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> -32768")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 127") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 127")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 127")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> -128") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> -128")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> -128")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 0") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 0")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 0")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 2017") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 2017")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 2017")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_decimal <> 2147868.65536") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_decimal <> 2147868.65536")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_decimal <> 2147868.65536")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select  count(1)  from full_data_type_table  where tp_binary <> null") {
    val r1 = querySpark("select  count(1)  from full_data_type_table  where tp_binary <> null")
    val r2 = querySpark("select  count(1)  from full_data_type_table_j  where tp_binary <> null")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}