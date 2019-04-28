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
        List("tp_time", "bigint", "true", null),
        List("tp_enum", "string", "true", null),
        List("tp_set", "string", "true", null)
      )
    setCurrentDatabase("tispark_test")
    spark.sql("desc full_data_type_table").explain(true)
    explainAndRunTest("desc full_data_type_table", skipJDBC = true, rTiDB = tidbDescTable)
    spark.sql("desc extended full_data_type_table").explain()
    spark.sql("desc extended full_data_type_table").show(200, truncate = false)
    spark.sql("desc formatted full_data_type_table").show(200, truncate = false)
    setCurrentDatabase("default")
    spark.sql("drop table if exists t")
    spark.sql("create table t(a int)")
    spark.sql("desc t").show
    spark.sql("desc formatted t").show
    spark.sql("drop table if exists t")
  }

  test("test support show columns") {
    val columnNames = List(
      List("id_dt"),
      List("tp_varchar"),
      List("tp_datetime"),
      List("tp_blob"),
      List("tp_binary"),
      List("tp_date"),
      List("tp_timestamp"),
      List("tp_year"),
      List("tp_bigint"),
      List("tp_decimal"),
      List("tp_double"),
      List("tp_float"),
      List("tp_int"),
      List("tp_mediumint"),
      List("tp_real"),
      List("tp_smallint"),
      List("tp_tinyint"),
      List("tp_char"),
      List("tp_nvarchar"),
      List("tp_longtext"),
      List("tp_mediumtext"),
      List("tp_text"),
      List("tp_tinytext"),
      List("tp_bit"),
      List("tp_time"),
      List("tp_enum"),
      List("tp_set")
    )
    setCurrentDatabase("tispark_test")
    explainAndRunTest(
      "show columns from full_data_type_table",
      skipJDBC = true,
      rTiDB = columnNames
    )
    runTest(
      s"show columns from ${dbPrefix}tispark_test.full_data_type_table",
      skipJDBC = true,
      rTiDB = columnNames
    )
  }

  test("test support create table like") {
    setCurrentDatabase("default")
    spark.sql("drop table if exists t")
    spark.sql(s"create table t like ${dbPrefix}tpch_test.nation").show
    spark.sql("show tables").show
    checkSparkResultContains("show tables", List("default", "t", "false"))
    spark.sql("show create table t").show(false)
  }

  override def beforeAll(): Unit = {
    enableHive = true
    super.beforeAll()
  }
}
