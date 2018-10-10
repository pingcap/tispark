package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.BaseTiSparkSuite

class CatalogTestSuite extends BaseTiSparkSuite {

  test("test new catalog") {
    runTest(s"select count(*) from ${dbPrefix}tispark_test.full_data_type_table")
    setCurrentDatabase("tispark_test")
    runTest(s"select count(*) from full_data_type_table")
  }

  test("test db prefix") {
    setCurrentDatabase("default")
    explainAndRunTest(s"select count(*) from ${dbPrefix}tispark_test.full_data_type_table")
  }

  test("test show databases/tables") {
    spark.sql("show databases").show(false)
    spark.sql(s"show databases like '$dbPrefix*'").show(false)
    setCurrentDatabase("tispark_test")
    spark.sql("show tables").show(false)
    spark.sql("show tables like '*_idx'").show(false)
    spark.sql("show tables like '*_j'").show(false)
    spark.sql(s"show tables from $dbPrefix$tpchDBName").show(false)
  }
}
