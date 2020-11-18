/*
 * Copyright 2019 PingCAP, Inc.
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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.{AnalysisException, BaseTiSparkTest}

class CatalogTestSuite extends BaseTiSparkTest {
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
    val qSparkDatabase = if (catalogPluginMode) {
      "tidb_catalog.tispark_test"
    } else {
      s"${dbPrefix}tispark_test"
    }
    compSparkWithTiDB(
      qSpark = s"select count(*) from $qSparkDatabase.full_data_type_table",
      qTiDB = s"select count(*) from tispark_test.full_data_type_table")
    setCurrentDatabase("tispark_test")
    runTest(s"select count(*) from full_data_type_table")
  }

  test("test db prefix") {
    if (catalogPluginMode) {
      cancel
    }

    setCurrentDatabase("default")
    compSparkWithTiDB(
      qSpark = s"select count(*) from ${dbPrefix}tispark_test.full_data_type_table",
      qTiDB = s"select count(*) from tispark_test.full_data_type_table")
  }

  test("test explain") {
    setCurrentDatabase("tispark_test")
    assert(
      spark
        .sql("explain select id_dt from full_data_type_table1")
        .head
        .getString(0)
        .contains("AnalysisException"))
    assert(
      !spark
        .sql("explain select id_dt from full_data_type_table")
        .head
        .getString(0)
        .contains("AnalysisException"))
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
        List("tp_decimal", "decimal(38,18)", "true", null),
        List("tp_double", "double", "true", null),
        List("tp_float", "float", "true", null),
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
        List("tp_set", "string", "true", null))

    setCurrentDatabase("tispark_test")
    spark.sql("desc full_data_type_table").explain(true)
    spark.sql("desc extended full_data_type_table").explain()

    if (!catalogPluginMode) {
      explainAndRunTest("desc full_data_type_table", skipJDBC = true, rTiDB = tidbDescTable)

      spark.sql("drop view if exists v")
      spark.sql("create temporary view v as select * from full_data_type_table")
      explainAndRunTest("desc v", skipJDBC = true, rTiDB = tidbDescTable)
    }

    refreshConnections(true)
    setCurrentDatabase("default")
    spark.sql("drop table if exists t")
    spark.sql("create table t(a int)")
    spark.sql("desc t").show
    spark.sql("desc formatted t").show
    spark.sql("drop table if exists t")
  }

  test("test show hive partition table") {
    refreshConnections(true)
    setCurrentDatabase("default")
    spark.sql("drop table if exists salesdata")
    spark.sql(
      "create table salesdata (salesperson_id int, product_id int) partitioned by (date_of_sale string)")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sql("insert into table salesdata values(0, 1, '10-27-2017')")
    spark.sql("insert into table salesdata values(0, 1, '10-28-2017')")
    spark.sql("insert into table salesdata values(0, 1, '10-29-2017')")
    val partitionsRes =
      List(
        List("date_of_sale=10-27-2017"),
        List("date_of_sale=10-28-2017"),
        List("date_of_sale=10-29-2017"))
    runTest("show partitions salesdata", skipJDBC = true, rTiDB = partitionsRes)
    spark.sql("drop table if exists salesdata")
  }

  test("test support show columns") {
    if (catalogPluginMode) {
      cancel("SHOW COLUMNS is only supported with temp views or v1 tables")
    }

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
      List("tp_set"))

    spark.sql("create database d1")
    spark.sql("use d1")
    spark.sql("create table m(m1 int)")
    spark.sql("show columns from m").show(200, truncate = false)

    spark
      .sql("show columns from tidb_tispark_test.full_data_type_table")
      .show(200, truncate = false)
    spark.sql("use tidb_tispark_test")
    spark.sql("show columns from full_data_type_table").show(200, truncate = false)

    setCurrentDatabase("tispark_test")
    explainAndRunTest(
      "show columns from full_data_type_table",
      skipJDBC = true,
      rTiDB = columnNames)
    runTest(
      s"show columns from ${dbPrefix}tispark_test.full_data_type_table",
      skipJDBC = true,
      rTiDB = columnNames)
  }

  test("test support create table like") {
    if (catalogPluginMode) {
      cancel
    }

    setCurrentDatabase("default")
    spark.sql("drop table if exists t")
    spark.sql(s"create table t like ${dbPrefix}tpch_test.nation")
    spark.sql("show tables").show
    checkSparkResultContains("show tables", List("default", "t", "false"))
    spark.sql("show create table t").show(false)
  }

  test("test create table as select") {
    spark.sql("show databases").show(false)
    tidbStmt.execute("DROP TABLE IF EXISTS `test1`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test2`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test3`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test4`")
    tidbStmt.execute(
      "CREATE TABLE `test1` (`id` int primary key, `c1` int, `c2` int, KEY idx(c1, c2))")
    tidbStmt.execute("CREATE TABLE `test2` (`id` int, `c1` int, `c2` int)")
    tidbStmt.execute("CREATE TABLE `test3` (`id` int, `c1` int, `c2` int, KEY idx(c2))")
    tidbStmt.execute(
      "CREATE TABLE `test4` (`id` int primary key, `c1` int, `c2` int, KEY idx(c2))")
    tidbStmt.execute(
      "insert into test1 values(1, 2, 3), /*(1, 3, 2), */(2, 2, 4), (3, 1, 3), (4, 2, 1)")
    tidbStmt.execute(
      "insert into test2 values(1, 2, 3), (1, 2, 4), (2, 1, 4), (3, 1, 3), (4, 3, 1)")
    tidbStmt.execute(
      "insert into test3 values(1, 2, 3), (2, 1, 3), (2, 1, 4), (3, 2, 3), (4, 2, 1)")
    tidbStmt.execute("insert into test4 values(1, 2, 3), (2, 3, 1), (3, 3, 2), (4, 1, 3)")
    refreshConnections(isHiveEnabled = true)

    val df =
      spark.sql("""
                  |select t1.*, (
                  |	select count(*)
                  |	from test2
                  |	where id > 1
                  |) count, t1.c1 t1_c1, t2.c1 t2_c1, t3.id t3_id,
                  | t3.c1 t3_c1, t3.c2 t3_c2, t4.c3 t4_c3, t5.c2 t5_c2
                  |from (
                  |	select id, c1, c2
                  |	from test1) t1
                  |left join (
                  |	select id, c1, c2, c1 + coalesce(c2 % 2) as c3
                  |	from test2 where c1 + c2 > 3) t2
                  |on t1.id = t2.id
                  |left join (
                  |	select max(id) as id, min(c1) + c2 as c1, c2, count(*) as c3
                  |	from test3
                  |	where c2 <= 3 and exists (
                  |		select * from (
                  |			select id as c1 from test3)
                  |    where (
                  |      select max(id) from test1) = 4)
                  |	group by c2) t3
                  |on t1.id = t3.id
                  |left join (
                  |	select max(id) as id, min(c1) as c1,
                  |  max(c1) as c1, count(*) as c2, c2 as c3
                  |	from test3
                  |	where id not in (
                  |		select id
                  |		from test1
                  |		where c2 > 2)
                  |	group by c2) t4
                  |on t1.id = t4.id
                  |left join (
                  |	select * from test4
                  |	where c2 = (
                  |		select max(c2)
                  |		from test3
                  |		where c2 < 2)) t5
                  |on t1.id = t5.id
                """.stripMargin)
    df.createOrReplaceTempView("testTempView1")

    val df2 = spark.sql("""
                          |select t1.id t1_id,
                          | if (t1.c1 = 2,
                          |  t2.c1,
                          |  if (t1.c1 = 3, t3.c1, t1.c1)
                          | ) r_c1, t2.id t2_id, t3.id t3_id, t3.c2 t3_c2
                          |from (
                          |	select id, c2, c1
                          |	from test4) t1
                          |left join (
                          |	select if (c2 = 2, c2, if (c2 = 3, 30, 0)) id, id c1
                          |	from test4) t2
                          |on t1.id = t2.id
                          |left join (
                          | select
                          |  id, c1,
                          |  CASE c2
                          |   WHEN 1 THEN 100
                          |   WHEN 2 THEN 200
                          |   WHEN 3 THEN 300
                          |   ELSE 0
                          |  END c2
                          | from test4) t3
                          |on t1.id = t3.id
                          |where IF(
                          | t3.id in (
                          |  select max(t4.id)
                          |  from test4 t4
                          |	 where c2 <> (
                          |		select count(*)
                          |		from test4
                          |		where c2 < 2)
                          |  group by c2), true, ISNULL(t3.id))
      """.stripMargin)
    df2.createOrReplaceTempView("testTempView2")

    val df3 = spark.sql("""
                          |select t1.id t1_id, t1.c1 t1_c1, t2.id t2_id, t2.c1 t2_c1,
                          |       t3.id t3_id, t3.c1 t3_c1, t3.c2 t3_c2
                          |from (
                          |	select t1_id id, t2_id c1, t3_c2 c2
                          |	from testTempView2) t1
                          |left join (
                          |	select if (t3_c2 < 2, t3_c2, if (t3_c2 > 3, 30, 0)) id, t3_id c1
                          |	from testTempView2) t2
                          |on t1.id = t2.id
                          |left join (
                          | select
                          |  t2_id id, r_c1 c1,
                          |  CASE t3_c2
                          |   WHEN 1 THEN 100
                          |   WHEN 2 THEN 200
                          |   WHEN 3 THEN 300
                          |   ELSE 0
                          |  END c2
                          | from testTempView2) t3
                          |on t1.id = t3.id
                          |where IF(
                          | t3.id in (
                          |  select max(t3_id)
                          |  from testTempView2 t4
                          |	 where t2_id <> (
                          |		select count(*)
                          |		from testTempView2
                          |		where t2_id < 2)
                          |  group by t2_id), true, ISNULL(t3.id))
      """.stripMargin)
    df3.createOrReplaceTempView("testTempView3")

    setCurrentDatabase("default")
    spark.sql("drop table if exists testLogic1")
    spark.sql("drop table if exists testLogic2")
    spark.sql("drop table if exists testLogic3")
    spark.sql("create table testLogic1 as select * from testTempView1")
    spark.sql("create table testLogic2 as select * from testTempView3")
    spark.sql("create table testLogic3 as select * from testTempView2")
    df.explain
    df.show()
    df2.explain
    df2.show
    df3.explain
    df3.show
    spark.sql("insert overwrite table testLogic2 select * from testTempView3 where t1_id <= 3")
    spark.sql("insert overwrite table testLogic3 select * from testTempView2 where t1_id <= 3")
    spark.sql("drop table if exists testLogic1")
    spark.sql("drop table if exists testLogic2")
    spark.sql("drop table if exists testLogic3")
  }

  test("support desc table column") {
    if (catalogPluginMode) {
      cancel("Describing columns is not supported for v2 tables")
    }

    val expectedDescTableColumn =
      List(List("col_name", "id"), List("data_type", "bigint"), List("comment", "NULL"))
    val expectedDescExtendedTableColumn =
      List(
        List("col_name", "id"),
        List("data_type", "bigint"),
        List("comment", "NULL"),
        List("min", "NULL"),
        List("max", "NULL"),
        List("num_nulls", "NULL"),
        List("distinct_count", "NULL"),
        List("avg_col_len", "NULL"),
        List("max_col_len", "NULL"),
        List("histogram", "NULL"))

    tidbStmt.execute("DROP TABLE IF EXISTS `t`")
    tidbStmt.execute("""CREATE TABLE `t` (
        |  `id` bigint(20) DEFAULT NULL
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)

    setCurrentDatabase("tispark_test")
    // column does not exist
    intercept[AnalysisException] {
      spark.sql("desc t a").show()
    }

    explainAndRunTest("desc t id", skipJDBC = true, rTiDB = expectedDescTableColumn)
    explainAndRunTest(
      "desc extended t id",
      skipJDBC = true,
      rTiDB = expectedDescExtendedTableColumn)
    spark.sql("drop table if exists t")
  }

  test("test schema change") {
    val tableName = "catalog_test"
    tidbStmt.execute(s"DROP TABLE IF EXISTS `$tableName`")
    tidbStmt.execute(s"CREATE TABLE $tableName(c1 int)")
    val catalog = this.ti.tiSession.getCatalog
    val tableInfo1 = catalog.getTable(s"${dbPrefix}tispark_test", tableName)

    tidbStmt.execute(s"ALTER TABLE `$tableName` ADD COLUMN c2 INT;")
    val tableInfo2 = catalog.getTable(s"${dbPrefix}tispark_test", tableName)
    assert(tableInfo1.getUpdateTimestamp < tableInfo2.getUpdateTimestamp)

    tidbStmt.execute(s"insert into `$tableName` values (1, 2)")
    val tableInfo3 = catalog.getTable(s"${dbPrefix}tispark_test", tableName)
    assert(tableInfo2.getUpdateTimestamp == tableInfo3.getUpdateTimestamp)

    tidbStmt.execute(s"DROP TABLE IF EXISTS `$tableName`")
  }

  override def beforeAll(): Unit = {
    super.beforeAllWithHiveSupport()

    tidbStmt.execute(s"DROP TABLE IF EXISTS `catalog_test`")
  }
}
