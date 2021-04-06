/*
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
 */

package org.apache.spark.sql

import com.pingcap.tispark.TiConfigConst
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.functions.{col, sum}

class IssueTestSuite extends BaseTiSparkTest {

  test("test tiflash timestamp < 1970") {
    if (!enableTiFlashTest) {
      cancel("tiflash test not enabled")
    }

    tidbStmt.execute(s"""
         |DROP TABLE IF EXISTS `t`;
         |CREATE TABLE `t` (`col_timestamp0` timestamp,`col_bit` bit(1));
         |INSERT INTO `t` VALUES ('1969-12-31 17:00:01.0',b'0');
         |ALTER TABLE `t` SET TIFLASH REPLICA 1;
         |""".stripMargin)

    assert(checkLoadTiFlashWithRetry("t", Some("tispark_test")))
    explainAndRunTest("select col_timestamp0 from t where col_bit < 1", canTestTiFlash = true)
  }

  test("test clustered index read") {
    if (!supportClusteredIndex) {
      cancel("currently tidb instance does not support clustered index")
    }
    spark.sqlContext.setConf(TiConfigConst.USE_INDEX_SCAN_FIRST, "true")

    tidbStmt.execute("drop table if exists `tispark_test`.`clustered0`")

    tidbStmt.execute("""
        CREATE TABLE `tispark_test`.`clustered0` (
        |  `col_bigint` bigint(20) not null,
        |  `col_int0` int(11) not null,
        |  `col_int1` int(11) not null,
        |  UNIQUE KEY (`col_int0`),
        |  PRIMARY KEY (`col_bigint`) /*T![clustered_index] CLUSTERED */
        |  );
        |""".stripMargin)

    tidbStmt.execute("""
        |INSERT INTO `tispark_test`.`clustered0` VALUES (9223372036854775807,1,2);
        |""".stripMargin)
    val sql = "select * from clustered0"
    spark.sql(s"explain $sql").show(200, false)
    spark.sql(s"$sql").show(200, false)
    runTest(sql, skipJDBC = true)
    spark.sqlContext.setConf(TiConfigConst.USE_INDEX_SCAN_FIRST, "false")
  }

  test("partition table with date partition column name") {
    tidbStmt.execute("drop table if exists t1")
    tidbStmt.execute("""
        |CREATE TABLE t1 (
        |date date NOT NULL,
        |cost bigint(20) DEFAULT NULL)
        |PARTITION BY RANGE COLUMNS(date) (
        |PARTITION p20200404 VALUES LESS THAN ("2020-04-05"),
        |PARTITION p20200418 VALUES LESS THAN ("2020-04-19")
        |)
        |""".stripMargin)
    spark.sql("select `date`, sum(cost) from t1 where group by `date`").show()
  }

  test("count(a, b) with null values") {
    tidbStmt.execute("drop table if exists t1")
    tidbStmt.execute("create table t1(a int, b int)")
    tidbStmt.execute("insert into t1 values (1, 1), (2, null), (null, 3)")

    spark.sql("explain select count(a, b) from t1").show(false)
    spark.sql("select count(a, b) from t1").show(false)
    assert(1 == spark.sql("select count(a, b) from t1").collect().head.get(0))
  }

  test("count(*) with null values") {
    tidbStmt.execute("drop table if exists t1")
    tidbStmt.execute("create table t1(a int)")
    tidbStmt.execute("insert into t1 values (1), (null)")

    spark.sql("explain select count(*) from t1").show(false)
    spark.sql("select count(*) from t1").show(false)
    assert(2 == spark.sql("select count(*) from t1").collect().head.get(0))

    spark.sql("explain select count(*) from t1 where isnull(a)").show(false)
    spark.sql("select count(*) from t1 where isnull(a)").show(false)
    assert(1 == spark.sql("select count(*) from t1 where isnull(a)").collect().head.get(0))

    spark.sql("explain select count(*) from t1 where !isnull(a)").show(false)
    spark.sql("select count(*) from t1 where !isnull(a)").show(false)
    assert(1 == spark.sql("select count(*) from t1 where !isnull(a)").collect().head.get(0))
  }

  test("year type index plan") {
    tidbStmt.execute("drop table if exists t1")
    tidbStmt.execute("create table t1(id varchar(10),a year, index idx_a (a))")
    tidbStmt.execute(
      "insert into t1 values('ab',1970), ('ab',2010), ('ab',2012), ('ab',2014), ('ab',2018)")

    val query = "select * from t1 where a>2010 and a<2015"
    spark.sql(s"explain $query").show(false)
    spark.sql(query).show()

    assert(spark.sql(query).collect().length == 2)
  }

  test("Date type deals with timezone incorrectly") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("""
                       |CREATE TABLE `t` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `d` date DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                       |""".stripMargin)

    tidbStmt.execute("insert into t values(1, '2020-10-25'), (2, '2020-11-20')")

    explainAndRunTest(s"SELECT * FROM t")
  }

  test("Scala match error when predicate includes boolean type conversion") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("""
                       |CREATE TABLE `t` (
                       |  `id` varchar(11) DEFAULT NULL,
                       |  `flag` boolean DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                       |""".stripMargin)

    tidbStmt.execute("insert into t values('1', true), ('2', false), ('3', true), ('4', false)")

    explainAndRunTest(s"SELECT * FROM t WHERE FLAG = 'true' and SUBSTR(ID, 1, 2) = '1'")
  }

  test("large column number + double read") {
    tidbStmt.execute(
      resourceToString(
        s"issue/LargeColumn.sql",
        classLoader = Thread.currentThread().getContextClassLoader))
    refreshConnections()

    sql("explain select * from large_column where a = 1").show(false)
    sql("select * from large_column where a = 1").show(false)
    judge("select * from large_column where a = 1")
  }

  // https://github.com/pingcap/tispark/issues/1570
  test("Fix covering index generates incorrect plan when first column is not included in index") {
    tidbStmt.execute("DROP TABLE IF EXISTS tt")
    tidbStmt.execute("create table tt(c varchar(12), dt datetime, key dt_index(dt))")
    tidbStmt.execute("""
                       |INSERT INTO tt VALUES
                       | ('aa', '2007-09-01 00:00:00'),
                       | ('bb', '2007-09-02 00:00:00'),
                       | ('cc', '2007-09-03 00:00:00'),
                       | ('dd', '2007-09-04 00:00:00')""".stripMargin)

    runTest(
      "select count(*) from tt where dt >= timestamp '2007-09-01 00:00:01' and dt <=  timestamp '2007-09-04 00:00:00'")
  }

  test("fix partition table pruning when partdef contains bigint") {
    tidbStmt.execute("DROP TABLE IF EXISTS t")
    tidbStmt.execute(
      """
        |CREATE TABLE `t` (
        |`xh` int(11) DEFAULT NULL,
        |`pt` bigint(20) DEFAULT NULL
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
        |PARTITION BY RANGE ( `pt` ) (PARTITION p202112 VALUES LESS THAN (20211201000000)
        |)
      """.stripMargin)
    //error
    tidbStmt.execute("insert into t(xh, pt) values(1, 20211200000000)")

    //error
    tidbStmt.execute("insert into t(xh, pt) values(1, 202112)")

    sql("select xh, pt from t").show()
    sql("select xh, pt from t where xh=1").show()
  }

  // https://github.com/pingcap/tispark/issues/1186
  test("Consider nulls order when performing TopN") {
    // table `full_data_type_table` contains a single line of nulls
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_int, tp_bigint from full_data_type_table order by tp_int asc nulls last limit 2",
      qTiDB =
        "select id_dt, tp_int, tp_bigint from full_data_type_table order by tp_int is null asc, tp_int asc limit 2")
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_int, tp_bigint from full_data_type_table order by tp_int desc nulls first limit 2",
      qTiDB =
        "select id_dt, tp_int, tp_bigint from full_data_type_table order by tp_int is null desc, tp_int desc limit 2")
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_int, tp_bigint from full_data_type_table order by tp_int asc nulls first limit 2",
      qTiDB =
        "select id_dt, tp_int, tp_bigint from full_data_type_table order by tp_int asc limit 2")
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_int, tp_bigint from full_data_type_table order by tp_int desc nulls last limit 2",
      qTiDB =
        "select id_dt, tp_int, tp_bigint from full_data_type_table order by tp_int desc limit 2")
  }

  // https://github.com/pingcap/tispark/issues/1161
  test("No Match Column") {
    tidbStmt.execute("DROP TABLE IF EXISTS t_no_match_column")
    tidbStmt.execute("""
                       |CREATE TABLE `t_no_match_column` (
                       |  `json` text DEFAULT NULL
                       |)
      """.stripMargin)

    spark.sql("""
                |select temp44.id from (
                |    SELECT get_json_object(t.json,  '$.id') as id from t_no_match_column t
                |) as temp44 order by temp44.id desc
      """.stripMargin).explain(true)

    spark.sql("""
                |select temp44.id from (
                |    SELECT get_json_object(t.json,  '$.id') as id from t_no_match_column t
                |) as temp44 order by temp44.id desc limit 1
      """.stripMargin).explain(true)

    judge("select tp_int from full_data_type_table order by tp_int limit 20")
    judge("select tp_int g from full_data_type_table order by g limit 20")
  }

  // https://github.com/pingcap/tispark/issues/1147
  test("Fix Bit Type default value bug") {
    def runBitTest(bitNumber: Int, bitString: String): Unit = {
      tidbStmt.execute("DROP TABLE IF EXISTS t_origin_default_value")
      tidbStmt.execute(
        s"CREATE TABLE t_origin_default_value (id bigint(20) NOT NULL, approved bit($bitNumber) NOT NULL DEFAULT b'$bitString')")
      tidbStmt.execute("insert into t_origin_default_value(id) values (1), (2), (3)")
      judge("select * from t_origin_default_value")

      tidbStmt.execute("DROP TABLE IF EXISTS t_origin_default_value")
      tidbStmt.execute("CREATE TABLE t_origin_default_value (id bigint(20) NOT NULL)")
      tidbStmt.execute("insert into t_origin_default_value(id) values (1), (2), (3)")
      tidbStmt.execute(
        s"alter table t_origin_default_value add approved bit($bitNumber) NOT NULL DEFAULT b'$bitString'")
      judge("select * from t_origin_default_value")
    }

    "0" :: "1" :: Nil foreach { bitString =>
      runBitTest(1, bitString)
    }

    "000" :: "001" :: "010" :: "011" :: "100" :: "101" :: "110" :: "111" :: Nil foreach {
      bitString =>
        runBitTest(3, bitString)
    }

    "00000000" ::
      //"11111111" :: error because of https://github.com/pingcap/tidb/issues/12703
      //"10101010" :: error
      "01010101" ::
      Nil foreach { bitString =>
      runBitTest(8, bitString)
    }

    "0000000000000000000000000000000000000000000000000000000000000000" ::
      "1111111111111111111111111111111111111111111111111111111111111111" ::
      //"1010101010101010101010101010101001010101010101010101010101010101" :: error
      Nil foreach { bitString =>
      runBitTest(64, bitString)
    }
  }

  // https://github.com/pingcap/tispark/issues/1083
  test("TiSpark Catalog has no delay") {
    tidbStmt.execute("drop table if exists catalog_delay")
    tidbStmt.execute("create table catalog_delay(c1 bigint)")
    refreshConnections()
    val tiTableInfo1 =
      this.ti.tiSession.getCatalog.getTable(s"${dbPrefix}tispark_test", "catalog_delay")
    assert("catalog_delay".equals(tiTableInfo1.getName))
    assert("c1".equals(tiTableInfo1.getColumns.get(0).getName))

    tidbStmt.execute("drop table if exists catalog_delay")
    tidbStmt.execute("create table catalog_delay(c2 bigint)")
    val tiTableInfo2 =
      this.ti.tiSession.getCatalog.getTable(s"${dbPrefix}tispark_test", "catalog_delay")
    assert("catalog_delay".equals(tiTableInfo2.getName))
    assert("c2".equals(tiTableInfo2.getColumns.get(0).getName))

    assert(tiTableInfo1.getId != tiTableInfo2.getId)
  }

  // https://github.com/pingcap/tispark/issues/1039
  test("Distinct without alias throws NullPointerException") {
    tidbStmt.execute("drop table if exists t_distinct_alias")
    tidbStmt.execute("create table t_distinct_alias(c1 bigint);")
    tidbStmt.execute("insert into t_distinct_alias values (2), (3), (2);")

    val sqls = "select distinct(c1) as d, 1 as w from t_distinct_alias" ::
      "select c1 as d, 1 as w from t_distinct_alias group by c1" ::
      "select c1, 1 as w from t_distinct_alias group by c1" ::
      "select distinct(c1), 1 as w from t_distinct_alias" ::
      Nil

    for (sql <- sqls) {
      explainTestAndCollect(sql)
      compSparkWithTiDB(sql)
    }
  }

  test("cannot resolve column name when specifying table.column") {
    spark.sql("select full_data_type_table.id_dt from full_data_type_table").explain(true)
    judge("select full_data_type_table.id_dt from full_data_type_table")
    spark
      .sql(
        "select full_data_type_table.id_dt from full_data_type_table join full_data_type_table_idx on full_data_type_table.id_dt = full_data_type_table_idx.id_dt")
      .explain(true)
    judge(
      "select full_data_type_table.id_dt from full_data_type_table join full_data_type_table_idx on full_data_type_table.id_dt = full_data_type_table_idx.id_dt")
  }

  test("test date") {
    judge("select tp_date from full_data_type_table where tp_date >= date '2065-04-19'")
    judge(
      "select tp_date, tp_datetime, id_dt from full_data_type_table where tp_date <= date '2065-04-19' order by id_dt limit 10")
    judge(
      "select tp_date, tp_datetime, tp_timestamp from full_data_type_table_idx where tp_date < date '2017-11-02' order by id_dt")
    judge(
      "select cast(tp_datetime as date) cast_datetime, date(tp_datetime) date_datetime, tp_datetime from full_data_type_table where tp_date < date '2017-11-02'")
  }

  test("Test count") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t`")
    tidbStmt.execute("""CREATE TABLE `t` (
        |  `a` int(11) DEFAULT NULL
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)
    tidbStmt.execute("insert into t values(1),(2),(3),(4),(null)")

    assert(spark.sql("select * from t limit 10").count() == 5)
    assert(spark.sql("select a from t limit 10").count() == 5)

    judge("select count(1) from (select a from t order by a limit 10) e", checkLimit = false)
    judge("select count(a) from (select a from t order by a limit 10) e", checkLimit = false)
    judge("select count(1) from (select * from t order by a limit 10) e", checkLimit = false)
  }

  test("Test sql with limit without order by") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t`")
    tidbStmt.execute("""CREATE TABLE `t` (
        |  `a` int(11) DEFAULT NULL,
        |  `b` decimal(15,2) DEFAULT NULL
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)
    tidbStmt.execute("insert into t values(1,771.64),(2,378.49),(3,920.92),(4,113.97)")

    assert(try {
      judge("select a, max(b) from t group by a limit 2", canTestTiFlash = true)
      false
    } catch {
      case _: Throwable => true
    })
  }

  ignore("Test index task downgrade") {
    val sqlConf = ti.sparkSession.sqlContext.conf
    val prevRegionIndexScanDowngradeThreshold =
      sqlConf.getConfString(TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD, "10000")
    sqlConf.setConfString(TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD, "10")
    val q = "select * from full_data_type_table_idx where tp_int < 1000000000 and tp_int > 0"
    explainAndRunTest(q, q.replace("full_data_type_table_idx", "full_data_type_table_idx_j"))
    sqlConf.setConfString(
      TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD,
      prevRegionIndexScanDowngradeThreshold)
  }

  test("Test index task batch size") {
    val tiConf = ti.tiConf
    val prevIndexScanBatchSize = tiConf.getIndexScanBatchSize
    tiConf.setIndexScanBatchSize(5)
    val q = "select * from full_data_type_table_idx where tp_int < 1000000000 and tp_int > 0"
    explainAndRunTest(q, q.replace("full_data_type_table_idx", "full_data_type_table_idx_j"))
    tiConf.setIndexScanBatchSize(prevIndexScanBatchSize)
  }

  test("TISPARK-21 count(1) when single read results in DAGRequestException") {
    tidbStmt.execute("DROP TABLE IF EXISTS `single_read`")
    tidbStmt.execute("""CREATE TABLE `single_read` (
        |  `c1` int(11) NOT NULL,
        |  `c2` int(11) NOT NULL,
        |  `c3` int(11) NOT NULL,
        |  `c4` int(11) NOT NULL,
        |  `c5` int(11) DEFAULT NULL,
        |  PRIMARY KEY (`c3`,`c2`),
        |  KEY `c4` (`c4`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin""".stripMargin)
    tidbStmt.execute(
      "insert into single_read values(1, 1, 1, 2, null), (1, 2, 1, 1, null), (2, 1, 3, 2, null), (2, 2, 2, 1, 0)")

    judge("select count(1) from single_read")
    judge("select count(c1) from single_read")
    judge("select count(c2) from single_read")
    judge("select count(c5) from single_read")
    judge("select count(1) from single_read where c2 < 2")
    judge("select c2, c3 from single_read")
    judge("select c3, c4 from single_read")
  }

  ignore("test sum rewriting logic") {
    // only test numeric types. Spark will raise analysis exception if we
    // perform sum aggregation over non-numeric types.
    judge("select sum(tp_decimal) from full_data_type_table")
    judge("select sum(tp_real) from full_data_type_table")
    judge("select sum(tp_double) from full_data_type_table")
    judge("select sum(tp_int) from full_data_type_table")
    judge("select sum(id_dt) from full_data_type_table")
    judge("select max(tp_float) from full_data_type_table")
    judge("select min(tp_float) from full_data_type_table")
  }

  test("TISPARK-16 fix excessive dag column") {
    if (enableTiFlashTest) {
      cancel("ignored in tiflash test")
    }
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("DROP TABLE IF EXISTS `t2`")
    tidbStmt.execute("""CREATE TABLE `t1` (
        |         `c1` bigint(20) DEFAULT NULL,
        |         `k2` int(20) DEFAULT NULL,
        |         `k1` varchar(32) DEFAULT NULL
        |         ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin""".stripMargin)
    tidbStmt.execute("""CREATE TABLE `t2` (
        |         `c2` bigint(20) DEFAULT NULL,
        |         `k2` int(20) DEFAULT NULL,
        |         `k1` varchar(32) DEFAULT NULL
        |         ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin""".stripMargin)
    tidbStmt.execute("insert into t1 values(1, 201707, 'aa'), (2, 201707, 'aa')")
    tidbStmt.execute("insert into t2 values(2, 201707, 'aa')")

    // Note: Left outer join for DataSet is different from that in mysql.
    // The result of DataSet[a, b, c] left outer join DataSet[d, b, c]
    // on join key(b, c) will be DataSet[b, c, a, d]
    val t1_df = spark.sql("select * from t1")
    val t1_group_df = t1_df.groupBy("k1", "k2").agg(sum("c1").alias("c1"))
    val t2_df = spark.sql("select * from t2")
    t2_df.printSchema()
    t2_df.show
    val join_df = t1_group_df.join(t2_df, Seq("k1", "k2"), "left_outer")
    join_df.printSchema()
    join_df.explain
    join_df.show
    val filter_df = join_df.filter(col("c2").isNotNull)
    filter_df.show
    val project_df = join_df.select("k1", "k2", "c2")
    project_df.show
  }

  // https://github.com/pingcap/tispark/issues/255
  test("Group by with first") {
    setCurrentDatabase(tpchDBName)
    val q1 =
      """
        |select
        |   l_returnflag
        |from
        |   lineitem
        |where
        |   l_shipdate <= date '1998-12-01'
        |group by
        |   l_returnflag""".stripMargin
    val q2 =
      """
        |select
        |   avg(l_quantity)
        |from
        |   lineitem
        |where
        |   l_shipdate >= date '1994-01-01'
        |group by
        |   l_partkey""".stripMargin
    // Should not throw any exception
    runTest(q1, skipTiDB = true)
    runTest(q2, skipTiDB = true)
    setCurrentDatabase("tispark_test")
  }

  // https://github.com/pingcap/tispark/issues/162
  test("select count(something + constant) reveals NPE on master branch") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(c1 int not null)")
    tidbStmt.execute("insert into t values(1)")
    tidbStmt.execute("insert into t values(2)")
    tidbStmt.execute("insert into t values(4)")
    loadTestData()
    runTest("select count(c1) from t")
    runTest("select count(c1 + 1) from t")
    runTest("select count(1 + c1) from t")
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(c1 int not null, c2 int not null)")
    tidbStmt.execute("insert into t values(1, 4)")
    tidbStmt.execute("insert into t values(2, 2)")
    loadTestData()
    runTest("select count(c1 + c2) from t")
  }

  // https://github.com/pingcap/tispark/issues/496
  test("NPE when count(1) on empty table") {
    tidbStmt.execute("DROP TABLE IF EXISTS `tmp_empty_tbl`")
    tidbStmt.execute("CREATE TABLE `tmp_empty_tbl` (`c1` varchar(20))")
    judge("select count(1) from `tmp_empty_tbl`")
    judge("select cast(count(1) as char(20)) from `tmp_empty_tbl`")
  }

  test("test push down filters when using index double read") {
    if (enableTiFlashTest) {
      cancel("ignored in tiflash test")
    }
    explainTestAndCollect(
      "select id_dt, tp_int, tp_double, tp_varchar from full_data_type_table_idx limit 10")
    explainTestAndCollect(
      "select id_dt, tp_int, tp_double, tp_varchar from full_data_type_table_idx where tp_int > 200 limit 10")
    explainTestAndCollect(
      "select id_dt, tp_int, tp_double, tp_varchar from full_data_type_table_idx where tp_int > 200 order by tp_varchar limit 10")
    explainTestAndCollect(
      "select max(tp_double) from full_data_type_table_idx where tp_int > 200 group by tp_bigint limit 10")
  }

  test("unsigned bigint as group by column") {
    if (enableTiFlashTest) {
      cancel("ignored in tiflash test")
    }
    tidbStmt.execute("drop table if exists table_group_by_bigint")
    tidbStmt.execute("""
                       |CREATE TABLE `table_group_by_bigint` (
                       |  `a` int(11) NOT NULL,
                       |  `b` bigint(20) UNSIGNED DEFAULT NULL,
                       |  `c` bigint(20) UNSIGNED DEFAULT NULL,
                       |  KEY idx(b)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
      """.stripMargin)
    tidbStmt.execute(
      "insert into table_group_by_bigint values(1, 2, 18446744073709551615), (2, 18446744073709551615, 18446744073709551614), (3, 18446744073709551615, 5), (4, 18446744073709551614, 18446744073709551614)")
    explainTestAndCollect("select sum(a) from table_group_by_bigint group by b")
    explainTestAndCollect("select sum(a) from table_group_by_bigint where c > 0 group by b")
    explainTestAndCollect("select sum(b) from table_group_by_bigint group by c")
    explainTestAndCollect("select sum(a) from table_group_by_bigint group by b")
    explainTestAndCollect("select b from table_group_by_bigint group by b")
    explainTestAndCollect(
      "select b from table_group_by_bigint where c=18446744073709551614 group by b")
  }

  test("test large amount of tables") {
    tidbStmt.execute("DROP DATABASE IF EXISTS large_amount_tables")
    tidbStmt.execute("CREATE DATABASE large_amount_tables")

    val size = 2000
    var sqls = ""
    (1 to size).foreach { i =>
      val sql = s"create table large_amount_tables.t$i(c1 bigint); "
      sqls = sqls + sql
    }
    tidbStmt.execute(sqls)
    sql(s"use ${dbPrefix}large_amount_tables")
    val df = sql("show tables")
    assert(df.count() >= size)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("DROP DATABASE IF EXISTS large_amount_tables")
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists tmp_debug")
      tidbStmt.execute("drop table if exists t1")
      tidbStmt.execute("drop table if exists t2")
      tidbStmt.execute("drop table if exists single_read")
      tidbStmt.execute("drop table if exists set_t")
      tidbStmt.execute("drop table if exists enum_t")
      tidbStmt.execute("drop table if exists table_group_by_bigint")
      tidbStmt.execute("drop table if exists catalog_delay")
      tidbStmt.execute("drop table if exists t_origin_default_value")
      tidbStmt.execute("drop table if exists t_no_match_column")
    } finally {
      super.afterAll()
    }
}
