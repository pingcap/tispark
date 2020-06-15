/*
 * Copyright 2020 PingCAP, Inc.
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
 */

package org.apache.spark.sql

class OutputOffsetsSuite extends BaseTiSparkTest {

  test("test the correctness of setting output-offsets in dag request") {
    judge("select sum(tp_double) from full_data_type_table group by id_dt, tp_float")
    judge("select avg(tp_double) from full_data_type_table group by id_dt, tp_float")
    judge("select count(tp_double) from full_data_type_table group by id_dt, tp_float")
    judge("select min(tp_double) from full_data_type_table group by id_dt, tp_float")
    judge("select max(tp_double) from full_data_type_table group by id_dt, tp_float")
    judge(
      "select sum(tp_double), avg(tp_float) from full_data_type_table group by id_dt, tp_float")
    judge(
      "select count(tp_double), avg(tp_float) from full_data_type_table group by id_dt, tp_float")
  }
}
