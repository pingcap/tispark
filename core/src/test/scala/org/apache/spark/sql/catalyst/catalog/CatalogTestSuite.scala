package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.BaseTiSparkSuite

class CatalogTestSuite extends BaseTiSparkSuite {

  test("test new catalog") {
    runTest(s"select count(*) from ${dbPrefix}tispark_test.full_data_type_table")
    spark.sql(s"use ${dbPrefix}tispark_test")
    runTest(s"select count(*) from full_data_type_table")
  }

  test("test db prefix") {
    spark.sql("use default")
    explainAndRunTest(s"select count(*) from ${dbPrefix}tispark_test.full_data_type_table")
  }
}
