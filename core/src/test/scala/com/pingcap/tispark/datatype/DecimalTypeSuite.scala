package com.pingcap.tispark.datatype

import org.apache.spark.sql.BaseTiSparkTest

class DecimalTypeSuite extends BaseTiSparkTest {
  test("test decimal reading logic") {
    judge("select tp_decimal from full_data_type_table_idx")
    judge("select tp_decimal from full_data_type_table")
  }

  test("test bigint reading logic") {
    judge("select tp_bigint from full_data_type_table_idx")
    judge("select tp_bigint from full_data_type_table")
  }

  test("test decimal reading logic with fraction 9") {
    tidbStmt.execute("drop table if exists tbl_dec")
    tidbStmt.execute("create table tbl_dec(d decimal(28, 9))")

    tidbStmt.execute("insert into tbl_dec(d) values(1.1111111111111111), (2.2)")
    tidbStmt.execute("insert into tbl_dec(d) values(000.1111111111111111), (2.2)")

    judge("select * from tbl_dec")
  }

  test("test decimal reading logic with fraction 4") {
    tidbStmt.execute("drop table if exists tbl_dec")
    tidbStmt.execute("create table tbl_dec(d decimal(28, 4))")

    tidbStmt.execute("insert into tbl_dec(d) values(1.1111111111111111), (2.2)")

    judge("select * from tbl_dec")
  }

  //TODO figure how to display decimal correctly in TiSpark
  test("test decimal reading logic with fraction 10") {
    tidbStmt.execute("drop table if exists tbl_dec")
    tidbStmt.execute("create table tbl_dec(d decimal(38, 10))")

    tidbStmt.execute("insert into tbl_dec(d) values(1.1111111111111111), (2.2)")

    judge("select * from tbl_dec")
  }

  test("test decimal reading logic with fraction 17") {
    tidbStmt.execute("drop table if exists tbl_dec")
    tidbStmt.execute("create table tbl_dec(d decimal(38, 17))")

    tidbStmt.execute("insert into tbl_dec(d) values(1.1111111111111111), (2.2)")

    judge("select * from tbl_dec")
  }

  test("test decimal reading logic with fraction 30") {
    tidbStmt.execute("drop table if exists tbl_dec")
    tidbStmt.execute("create table tbl_dec(d decimal(38, 30))")

    tidbStmt.execute("insert into tbl_dec(d) values(1.1111111111111111), (2.2)")

    judge("select * from tbl_dec")
  }
}
