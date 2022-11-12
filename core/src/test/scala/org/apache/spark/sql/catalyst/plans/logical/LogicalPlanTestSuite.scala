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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.logical

<<<<<<< HEAD
import com.pingcap.tikv.meta.TiTimestamp
=======
import com.pingcap.tikv.codec.KeyUtils
import com.pingcap.tikv.expression.ComparisonBinaryExpression.{greaterEqual, lessEqual}
import com.pingcap.tikv.expression.{ColumnRef, Constant, Expression, LogicalBinaryExpression}
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder
import com.pingcap.tikv.meta.TiDAGRequest
import com.pingcap.tikv.types.{DataType, DataTypeFactory, IntegerType, MySQLType}
import com.pingcap.tikv.util.ConvertUpstreamUtils
>>>>>>> 0fccb5a40 (fix statistics (#2578))
import org.apache.spark.sql.catalyst.plans.BasePlanTest

class LogicalPlanTestSuite extends BasePlanTest {

  // When statistics is enabled, the plan will be different caused by CBO.
  // In order to get the exact result, we need to disable statistics.
  override def beforeAll(): Unit = {
    _isStatisticsEnabled = false
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    _isStatisticsEnabled = true
    try {
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists t1")
      tidbStmt.execute("drop table if exists test1")
      tidbStmt.execute("drop table if exists test2")
      tidbStmt.execute("drop table if exists test3")
    } finally {
      super.afterAll()
    }
  }

  // https://github.com/pingcap/tispark/issues/2328
  test("limit push down fail in df.show") {
    tidbStmt.execute("DROP TABLE IF EXISTS `test_l`")
    tidbStmt.execute(
      "CREATE TABLE `test_l` (`id` int , PRIMARY KEY(`id`)/*T![clustered_index] CLUSTERED */)")
    // we need to exclude combineLimits to simulate df.show's plan
    spark.sql(
      "SET spark.sql.optimizer.excludedRules=org.apache.spark.sql.catalyst.optimizer.CombineLimits")
    val df = spark.sql("select id from test_l limit 1").limit(21)
    val DAGRequest = extractDAGRequests(df).head.toString
    spark.sql("SET spark.sql.optimizer.excludedRules=''")
    if (!DAGRequest.contains("Limit")) {
      fail("Limit not push down")
    }
  }

  test("fix Residual Filter containing wrong info") {
    val df = spark
      .sql("select * from full_data_type_table where tp_mediumint > 0 order by tp_int")
    if (extractDAGRequests(df).head.toString.contains("Residual Filters")) {
      fail("Residual Filters should not appear")
    }
  }

  test("test timestamp in logical plan") {
    tidbStmt.execute("DROP TABLE IF EXISTS `test1`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test2`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test3`")
    tidbStmt.execute(
      "CREATE TABLE `test1` (`id` int primary key, `c1` int, `c2` int, KEY idx(c1, c2))")
    tidbStmt.execute("CREATE TABLE `test2` (`id` int, `c1` int, `c2` int)")
    tidbStmt.execute("CREATE TABLE `test3` (`id` int, `c1` int, `c2` int, KEY idx(c2))")
    tidbStmt.execute(
      "insert into test1 values(1, 2, 3), /*(1, 3, 2), */(2, 2, 4), (3, 1, 3), (4, 2, 1)")
    tidbStmt.execute(
      "insert into test2 values(1, 2, 3), (1, 2, 4), (2, 1, 4), (3, 1, 3), (4, 3, 1)")
    tidbStmt.execute(
      "insert into test3 values(1, 2, 3), (2, 1, 3), (2, 1, 4), (3, 2, 3), (4, 2, 1)")
    refreshConnections()
    val df =
      spark.sql("""
                  |select t1.*, (
                  |	select count(*)
                  |	from test2
                  |	where id > 1
                  |), t1.c1, t2.c1, t3.*, t4.c3
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
                  |	select max(id) as id, min(c1) as c1, max(c1) as c1, count(*) as c2, c2 as c3
                  |	from test3
                  |	where id not in (
                  |		select id
                  |		from test1
                  |		where c2 > 2)
                  |	group by c2) t4
                  |on t1.id = t4.id
      """.stripMargin)

    var v: TiTimestamp = null

    def checkTimestamp(version: TiTimestamp): Unit =
      if (version == null) {
        fail("timestamp is not defined!")
      } else if (v == null) {
        println("initialize timestamp should be " + version.getVersion)
        v = version
      } else if (v.getVersion != version.getVersion) {
        fail("multiple timestamp found in plan")
      } else {
        println("check ok " + v.getVersion)
      }

    extractDAGRequests(df).map(_.getStartTs).foreach { checkTimestamp }
  }

  // https://github.com/pingcap/tispark/issues/1498
  test("test index scan failed to push down varchar") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute(
      """
        |CREATE TABLE `t` (
        |  `id` int(11) NOT NULL PRIMARY KEY,
        |  `artical_id` varchar(255) DEFAULT NULL,
        |  `c` bigint(20) UNSIGNED DEFAULT NULL,
        |  `last_modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        |  KEY `artical_id`(`artical_id`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)
    tidbStmt.execute(
      "insert into t values(1, 'abc', 18446744073709551615, '2020-06-06 00:00:00'), (2, 'abcdefg', 18446744073709551614, '2020-06-26 00:00:00'), (3, 'acdgga', 5, '2020-06-25 00:00:00'), (4, 'abcdefgh', 18446744073709551614, '2020-06-12 00:00:00'), (5, 'aaabcd', 10, '2020-06-16 00:00:00')")
    tidbStmt.execute("analyze table t")
    val df = spark.sql(
      "select artical_id from t where artical_id = 'abcdefg' and last_modify_time > '2020-06-22 00:00:00'")
    checkIsIndexScan(df, "t")
    checkIndex(df, "artical_id")
  }

<<<<<<< HEAD
  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists test1")
      tidbStmt.execute("drop table if exists test2")
      tidbStmt.execute("drop table if exists test3")
    } finally {
      super.afterAll()
    }
=======
  test("test physical plan explain which table with common handle") {
    if (!ConvertUpstreamUtils.isTiKVVersionGreatEqualThanVersion(
        this.ti.clientSession.getTiKVSession.getPDClient,
        "5.0.0")) {
      cancel("TiDB version must bigger or equal than 5.0")
    }
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""
        |CREATE TABLE `t1` (
        |  `a` BIGINT(20) NOT NULL,
        |  `b` varchar(255) NOT NULL,
        |  `c` varchar(255) DEFAULT NULL,
        |   PRIMARY KEY (`b`) clustered
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)
    // TableScan with Selection and with RangeFilter.
    val df1 = spark.sql("select * from t1 where a>0 and b>'aa'")
    val dag1 = extractDAGRequests(df1).head
    val expectation1 =
      "== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ TableRangeScan: { RangeFilter: [%s], Range: [%s] }, Selection: [%s] }, startTs: %d}".trim
    val rangeFilter1 = dag1.getRangeFilter.toArray().mkString(", ")
    val selection1 = dag1.getFilters.toArray.mkString(", ")
    val myExpectation1 = expectation1.format(
      rangeFilter1,
      stringKeyRangeInDAG(dag1),
      selection1,
      dag1.getStartTs.getVersion)
    val sparkPhysicalPlan1 =
      df1.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectation1.equals(sparkPhysicalPlan1))

    // TableScan without Selection and with RangeFilter.
    val df2 = spark.sql("select * from t1 where b>'aa'")
    val dag2 = extractDAGRequests(df2).head
    val expectation2 =
      "== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ TableRangeScan: { RangeFilter: [%s], Range: [%s] } }, startTs: %d}".trim
    val rangeFilter2 = dag2.getRangeFilter.toArray().mkString(", ")
    val myExpectation2 =
      expectation2.format(rangeFilter2, stringKeyRangeInDAG(dag2), dag2.getStartTs.getVersion)
    val sparkPhysicalPlan2 =
      df2.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectation2.equals(sparkPhysicalPlan2))

    // TableScan with Selection and without RangeFilter.
    val df3 = spark.sql("select * from t1 where a>0")
    val dag3 = extractDAGRequests(df3).head
    val expectation3 =
      ("== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): { " +
        "TableRangeScan: { RangeFilter: [], Range: [%s] }, Selection: [%s] }, startTs: %d}").trim
    val selection3 = dag3.getFilters.toArray().mkString(", ")
    val myExpectation3 =
      expectation3.format(stringKeyRangeInDAG(dag3), selection3, dag3.getStartTs.getVersion)
    val sparkPhysicalPlan3 =
      df3.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectation3.equals(sparkPhysicalPlan3))
  }

  test("test physical plan explain which table with multi col common handle") {
    if (!ConvertUpstreamUtils.isTiKVVersionGreatEqualThanVersion(
        this.ti.clientSession.getTiKVSession.getPDClient,
        "5.0.0")) {
      cancel("TiDB version must bigger or equal than 5.0")
    }
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""
        |CREATE TABLE `t1` (
        |  `a` BIGINT(20) NOT NULL,
        |  `b` varchar(255) NOT NULL,
        |  `c` varchar(255) DEFAULT NULL,
        |   PRIMARY KEY (`b`,`a`) clustered
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)
    // TableScan with Selection and with RangeFilter.
    val df1 = spark.sql("select * from t1 where a>0 and b>'aa'")
    val dag1 = extractDAGRequests(df1).head
    val expectation1 =
      "== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ TableRangeScan: { RangeFilter: [%s], Range: [%s] }, Selection: [%s] }, startTs: %d}".trim
    val rangeFilter1 = dag1.getRangeFilter.toArray().mkString(", ")
    val selection1 = dag1.getFilters.toArray.mkString(", ")
    val myExpectation1 = expectation1.format(
      rangeFilter1,
      stringKeyRangeInDAG(dag1),
      selection1,
      dag1.getStartTs.getVersion)
    val sparkPhysicalPlan1 =
      df1.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectation1.equals(sparkPhysicalPlan1))

    // TableScan without Selection and with RangeFilter.
    val df2 = spark.sql("select * from t1 where b='aa' and a>0")
    val dag2 = extractDAGRequests(df2).head
    val expectation2 =
      "== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ TableRangeScan: { RangeFilter: [%s], Range: [%s] } }, startTs: %d}".trim
    val rangeFilter2 = dag2.getRangeFilter.toArray().mkString(", ")
    val myExpectation2 =
      expectation2.format(rangeFilter2, stringKeyRangeInDAG(dag2), dag2.getStartTs.getVersion)
    val sparkPhysicalPlan2 =
      df2.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectation2.equals(sparkPhysicalPlan2))

    // TableScan with Selection and without RangeFilter.
    val df3 = spark.sql("select * from t1 where a>0")
    val dag3 = extractDAGRequests(df3).head
    val expectation3 =
      ("== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): { " +
        "TableRangeScan: { RangeFilter: [], Range: [%s] }, Selection: [%s] }, startTs: %d}").trim
    val selection3 = dag3.getFilters.toArray().mkString(", ")
    val myExpectation3 =
      expectation3.format(stringKeyRangeInDAG(dag3), selection3, dag3.getStartTs.getVersion)
    val sparkPhysicalPlan3 =
      df3.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectation3.equals(sparkPhysicalPlan3))
  }

  def stringKeyRangeInDAG(dag: TiDAGRequest): String = {
    val sb = new StringBuilder()
    dag.getRangesMaps.values.forEach((vList: util.List[Coprocessor.KeyRange]) => {
      def foo(vList: util.List[Coprocessor.KeyRange]) = {
        import scala.collection.JavaConversions._
        for (range <- vList) { // LogDesensitization: show key range in coprocessor request in log
          sb.append(KeyUtils.formatBytesUTF8(range))
        }
      }

      foo(vList)
    })
    sb.toString()
  }
>>>>>>> 0fccb5a40 (fix statistics (#2578))
}
