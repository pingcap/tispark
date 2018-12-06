package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.BaseTiSparkSuite

class CatalogTestSuite extends BaseTiSparkSuite {

  test("test show databases/tables") {
    spark.sql("show databases").show(false)
    spark.sql(s"show databases like '$dbPrefix*'").show(false)
    setCurrentDatabase("tispark_test")
    spark.sql("show tables").show(false)
    spark.sql("show tables like '*_idx'").show(false)
    spark.sql("show tables like '*_j'").show(false)
    spark.sql(s"show tables from $dbPrefix$tpchDBName").show(false)
  }

  test("test new catalog") {
    setCurrentDatabase("default")
    runTest(s"select count(*) from ${dbPrefix}tispark_test.full_data_type_table")
    setCurrentDatabase("tispark_test")
    runTest(s"select count(*) from full_data_type_table")
  }

  test("test db prefix") {
    setCurrentDatabase("default")
    explainAndRunTest(s"select count(*) from ${dbPrefix}tispark_test.full_data_type_table")
  }

  test("test explain") {
    setCurrentDatabase("tispark_test")
    assert(
      spark
        .sql("explain select id_dt from full_data_type_table1")
        .head
        .getString(0)
        .contains("AnalysisException")
    )
    assert(
      !spark
        .sql("explain select id_dt from full_data_type_table")
        .head
        .getString(0)
        .contains("AnalysisException")
    )
  }

  test("test insert into table command") {
    setCurrentDatabase("default")
    spark.sql("drop table if exists hive_table")
    spark.sql("create table if not exists hive_table(c int)")
    spark.sql("insert into table hive_table values(1)")
    spark.sql("select * from hive_table").show()
  }
}
