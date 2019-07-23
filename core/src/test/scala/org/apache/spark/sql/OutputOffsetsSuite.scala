package org.apache.spark.sql

class OutputOffsetsSuite extends BaseTiSparkTest {

  test("test the correctness of setting output-offsets in dag request") {
    judge("select sum(tp_double) from full_data_type_table group by id_dt, tp_float");
    judge("select avg(tp_double) from full_data_type_table group by id_dt, tp_float");
    judge("select count(tp_double) from full_data_type_table group by id_dt, tp_float");
    judge("select min(tp_double) from full_data_type_table group by id_dt, tp_float");
    judge("select max(tp_double) from full_data_type_table group by id_dt, tp_float");
    judge(
      "select sum(tp_double), avg(tp_float) from full_data_type_table group by id_dt, tp_float"
    );
    judge(
      "select count(tp_double), avg(tp_float) from full_data_type_table group by id_dt, tp_float"
    );
  }
}
