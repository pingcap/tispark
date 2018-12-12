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

  test("support desc table") {
    val tidbDescTable =
      List(
        List("id_dt", "bigint", "false", null),
        List("tp_varchar", "string", "true", null),
        List("tp_datetime", "timestamp", "true", null),
        List("tp_blob", "binary", "true", null),
        List("tp_binary", "binary", "true", null),
        List("tp_date", "date", "true", null),
        List("tp_timestamp", "timestamp", "false", null),
        List("tp_year", "bigint", "true", null),
        List("tp_bigint", "bigint", "true", null),
        List("tp_decimal", "decimal(11,0)", "true", null),
        List("tp_double", "double", "true", null),
        List("tp_float", "double", "true", null),
        List("tp_int", "bigint", "true", null),
        List("tp_mediumint", "bigint", "true", null),
        List("tp_real", "double", "true", null),
        List("tp_smallint", "bigint", "true", null),
        List("tp_tinyint", "bigint", "true", null),
        List("tp_char", "string", "true", null),
        List("tp_nvarchar", "string", "true", null),
        List("tp_longtext", "string", "true", null),
        List("tp_mediumtext", "string", "true", null),
        List("tp_text", "string", "true", null),
        List("tp_tinytext", "string", "true", null),
        List("tp_bit", "bigint", "true", null),
        List("tp_time", "timestamp", "true", null),
        List("tp_enum", "bigint", "true", null),
        List("tp_set", "bigint", "true", null)
      )
    spark.sql("desc full_data_type_table").explain(true)
    explainAndRunTest("desc full_data_type_table", skipJDBC = true, rTiDB = tidbDescTable)
    spark.sql("desc extended full_data_type_table").explain()
    spark.sql("desc extended full_data_type_table").show(200, truncate = false)
    spark.sql("desc formatted full_data_type_table").show(200, truncate = false)
  }
}
