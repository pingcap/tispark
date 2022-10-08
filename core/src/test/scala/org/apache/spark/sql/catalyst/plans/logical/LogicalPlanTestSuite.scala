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

import com.pingcap.tikv.codec.KeyUtils
import com.pingcap.tikv.expression.ComparisonBinaryExpression.{greaterEqual, lessEqual}
import com.pingcap.tikv.expression.{ColumnRef, Constant, Expression, LogicalBinaryExpression}
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder
import com.pingcap.tikv.meta.TiDAGRequest
import com.pingcap.tikv.types.{DataType, DataTypeFactory, IntegerType, MySQLType}
import com.pingcap.tikv.util.ConvertUpstreamUtils
import com.pingcap.tispark.telemetry.TiSparkTeleInfo
import org.apache.spark.sql.catalyst.plans.BasePlanTest
import org.apache.spark.sql.execution.{ExplainMode, SimpleMode}
import org.scalatest.Matchers.{be, contain, convertToAnyShouldWrapper}
import org.tikv.common.meta.TiTimestamp
import org.tikv.kvproto.Coprocessor

import java.util

class LogicalPlanTestSuite extends BasePlanTest {

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

    extractDAGRequests(df).map(_.getStartTs).foreach {
      checkTimestamp
    }
  }

  // https://github.com/pingcap/tispark/issues/2290
  test("fix cannot encode row key with non-long type") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    if (ConvertUpstreamUtils.isTiKVVersionGreatEqualThanVersion(
        this.ti.clientSession.getTiKVSession.getPDClient,
        "5.0.0")) {
      tidbStmt.execute("""
          |CREATE TABLE `t1` (
          |  `a` BIGINT UNSIGNED  NOT NULL,
          |  `b` varchar(255) NOT NULL,
          |  `c` varchar(255) DEFAULT NULL,
          |  PRIMARY KEY (`a`) CLUSTERED
          |  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)
    } else {
      if (isEnableAlterPrimaryKey) {
        cancel("TiDB config alter-primary-key must be false")
      }
      tidbStmt.execute("""
                         |CREATE TABLE `t1` (
                         |  `a` BIGINT UNSIGNED  NOT NULL,
                         |  `b` varchar(255) NOT NULL,
                         |  `c` varchar(255) DEFAULT NULL,
                         |  PRIMARY KEY (`a`)
                         |  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)
    }
    tidbStmt.execute(
      " INSERT INTO t1 VALUES(0, 'aa', 'aa'), ( 9223372036854775807, 'bb', 'bb'), ( 9223372036854775808, 'cc', 'cc'), ( 18446744073709551615, 'dd', 'dd')")

    def checkResultAndExpression(
        sql: String,
        valueExpectation: List[String],
        expressionExpectation: Expression): Unit = {
      val df = spark.sql(sql)
      val dag = extractDAGRequests(df).head
      val rangeFilter = dag.getRangeFilter
      val value = df.select("a").collect().map(_(0).toString).toList
      value should contain theSameElementsAs valueExpectation
      if (rangeFilter.size() == 1) {
        rangeFilter.get(0).toString should be(expressionExpectation.toString)
      }
    }

    val holder = new InternalTypeHolder(
      MySQLType.TypeLonglong.getTypeCode,
      DataType.PriKeyFlag + DataType.UnsignedFlag + DataType.NoDefaultValueFlag,
      20,
      -1,
      "utf8_bin",
      "",
      null)

    val dataType = DataTypeFactory.of(holder)
    val situation1 = "SELECT a FROM t1 WHERE a >= 0 and a <= 9223372036854775807 order by a"
    val valueExpectation1 = List[String]("0", "9223372036854775807")
    val expressionExpectation1 = LogicalBinaryExpression.and(
      greaterEqual(new ColumnRef("a", dataType), Constant.create(0, dataType)),
      lessEqual(new ColumnRef("a", dataType), Constant.create(Long.MaxValue, dataType)))
    checkResultAndExpression(situation1, valueExpectation1, expressionExpectation1)

    val situation2 = "SELECT a FROM t1 WHERE a >= 0 and a<= 9223372036854775808 order by a"
    val valueExpectation2 = List[String]("0", "9223372036854775807", "9223372036854775808")
    val expressionExpectation2 = LogicalBinaryExpression.and(
      greaterEqual(new ColumnRef("a", dataType), Constant.create(0, dataType)),
      lessEqual(
        new ColumnRef("a", dataType),
        Constant.create(new java.math.BigDecimal("9223372036854775808"), dataType)))
    checkResultAndExpression(situation2, valueExpectation2, expressionExpectation2)

    val situation3 = "SELECT a FROM t1 WHERE a >= 0 order by a"
    val valueExpectation3 =
      List[String]("0", "9223372036854775807", "9223372036854775808", "18446744073709551615")
    val expressionExpectation3 =
      greaterEqual(new ColumnRef("a", dataType), Constant.create(0, IntegerType.BIGINT))
    checkResultAndExpression(situation3, valueExpectation3, expressionExpectation3)

    val situation4 =
      "SELECT a FROM t1 WHERE a >= 9223372036854775807 and a<=9223372036854775808 order by a"
    val valueExpectation4 = List[String]("9223372036854775807", "9223372036854775808")
    val expressionExpectation4 = LogicalBinaryExpression.and(
      greaterEqual(new ColumnRef("a", dataType), Constant.create(Long.MaxValue, dataType)),
      lessEqual(
        new ColumnRef("a", dataType),
        Constant.create(new java.math.BigDecimal("9223372036854775808"), dataType)))
    checkResultAndExpression(situation4, valueExpectation4, expressionExpectation4)

    val situation5 = "SELECT a FROM t1 WHERE a<=9223372036854775808 order by a"
    val valueExpectation5 = List[String]("0", "9223372036854775807", "9223372036854775808")
    val expressionExpectation5 = lessEqual(
      new ColumnRef("a", dataType),
      Constant.create(new java.math.BigDecimal("9223372036854775808"), dataType))
    checkResultAndExpression(situation5, valueExpectation5, expressionExpectation5)

    val situation6 = "SELECT a FROM t1 WHERE a <= 9223372036854775807 order by a"
    val valueExpectation6 = List[String]("0", "9223372036854775807")
    val expressionExpectation6 =
      lessEqual(new ColumnRef("a", dataType), Constant.create(Long.MaxValue, dataType))
    checkResultAndExpression(situation6, valueExpectation6, expressionExpectation6)

    val situation7 = "SELECT a FROM t1 WHERE a >= 9223372036854775808 and a<=9223372036854775809"
    val valueExpectation7 = List[String]("9223372036854775808")
    val expressionExpectation7 = LogicalBinaryExpression.and(
      greaterEqual(
        new ColumnRef("a", dataType),
        Constant.create(new java.math.BigDecimal("9223372036854775808"), dataType)),
      lessEqual(
        new ColumnRef("a", dataType),
        Constant.create(new java.math.BigDecimal("9223372036854775809"), dataType)))
    checkResultAndExpression(situation7, valueExpectation7, expressionExpectation7)

    val situation8 = "SELECT a FROM t1 WHERE a >= 9223372036854775808"
    val valueExpectation8 = List[String]("9223372036854775808", "18446744073709551615")
    val expressionExpectation8 = greaterEqual(
      new ColumnRef("a", dataType),
      Constant.create(new java.math.BigDecimal("9223372036854775808"), dataType))
    checkResultAndExpression(situation8, valueExpectation8, expressionExpectation8)

    val situation9 = "SELECT a FROM t1 order by a"
    val valueExpectation9 =
      List[String]("0", "9223372036854775807", "9223372036854775808", "18446744073709551615")
    checkResultAndExpression(situation9, valueExpectation9, null)
  }

  test("test physical plan explain which table without cluster index") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""
        |CREATE TABLE `t1` (
        |  `a` BIGINT(20) NOT NULL,
        |  `b` varchar(255) NOT NULL,
        |  `c` varchar(255) DEFAULT NULL
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)

    // TableScan with Selection and without RangeFilter.
    val df1 = spark.sql("select * from t1 where a>0 and b>'aa'")
    val dag1 = extractDAGRequests(df1).head
    val expectation1 =
      "== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ TableRangeScan: { RangeFilter: [], Range: [%s] }, Selection: [%s] }, startTs: %d}".trim
    val selection1 = dag1.getFilters.toArray().mkString(", ")
    val myExpectation1 =
      expectation1.format(stringKeyRangeInDAG(dag1), selection1, dag1.getStartTs.getVersion)
    val sparkPhysicalPlan1 =
      df1.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectation1.equals(sparkPhysicalPlan1))

    // TableScan with complex sql statements
    val df2 = spark.sql(
      "select * from t1 where a>0 or b > 'aa' or c<'cc' and c>'aa' order by(c) limit(10)")
    val dag2 = extractDAGRequests(df2).head
    val expectation2 =
      "[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ TableRangeScan: { RangeFilter: [], Range: [%s] }, Selection: [%s], " +
        "Order By: [c@VARCHAR(255) ASC] }, startTs: %d".trim
    val selection2 = dag2.getFilters.toArray().mkString(", ")
    val myExpectation2 =
      expectation2.format(stringKeyRangeInDAG(dag2), selection2, dag2.getStartTs.getVersion)
    assert(myExpectation2.equals(dag2.toString))
  }

  test("test physical plan explain which table with cluster index") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    if (ConvertUpstreamUtils.isTiKVVersionGreatEqualThanVersion(
        this.ti.clientSession.getTiKVSession.getPDClient,
        "5.0.0")) {
      tidbStmt.execute("""
          |CREATE TABLE `t1` (
          |  `a` BIGINT(20) NOT NULL,
          |  `b` varchar(255) NOT NULL,
          |  `c` varchar(255) DEFAULT NULL,
          |   PRIMARY KEY (`a`) clustered
          |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)
    } else {
      val config =
        tidbStmt.executeQuery("show config where type='tidb' and name='alter-primary-key'")
      if (!config.next() || config.getString(4).toLowerCase() == "true") {
        cancel("TiDB config alter-primary-key must be false")
      }
      tidbStmt.execute("""
                         |CREATE TABLE `t1` (
                         |  `a` INT(20) NOT NULL,
                         |  `b` varchar(255) NOT NULL,
                         |  `c` varchar(255) DEFAULT NULL,
                         |   PRIMARY KEY (`a`)
                         |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)
    }

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
    val df2 = spark.sql("select * from t1 where a>0")
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
    val df3 = spark.sql("select * from t1 where b>'aa'")
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

  test("test physical plan explain which table with cluster index and partition") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""
                       |CREATE TABLE `t1` (
                       |  `a` BIGINT(20) NOT NULL,
                       |  `b` varchar(255) NOT NULL,
                       |  `c` varchar(255) DEFAULT NULL,
                       |  PRIMARY KEY (a)
                       |)PARTITION BY RANGE (a) (
                       |    PARTITION p0 VALUES LESS THAN (6),
                       |    PARTITION p1 VALUES LESS THAN MAXVALUE
                       |  )
        """.stripMargin)
    // TableScan with Selection and with RangeFilter.
    val df = spark.sql("select b from t1 where t1.b>'aa'")
    val dags = extractDAGRequests(df)
    val expectation =
      "== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" + "" +
        "+- partition table[\n" +
        " TiKV CoprocessorRDD{[table: t1] TableReader, Columns: b@VARCHAR(255): " +
        "{ TableRangeScan: { RangeFilter: [], Range: [ partition: p0%s] }, " +
        "Selection: [%s] }, startTs: %d} \n " +
        "TiKV CoprocessorRDD{[table: t1] TableReader, Columns: b@VARCHAR(255): " + "" +
        "{ TableRangeScan: { RangeFilter: [], Range: [ partition: p1%s] }, " +
        "Selection: [%s] }, startTs: %d} \n" +
        "]".trim
    val selection0 = dags.head.getFilters.toArray.mkString(", ")
    val selection1 = dags(1).getFilters.toArray.mkString(", ")
    val myExpectationPlan = expectation.format(
      stringKeyRangeInDAG(dags.head),
      selection0,
      dags.head.getStartTs.getVersion,
      stringKeyRangeInDAG(dags(1)),
      selection1,
      dags(1).getStartTs.getVersion)
    val sparkPhysicalPlan =
      df.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectationPlan.equals(sparkPhysicalPlan))
  }

  test("test physical plan explain which table with secondary index") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""
                       |CREATE TABLE `t1` (
                       |`a` int(20)  NOT NULL,
                       |`b` varchar(255) NOT NULL,
                       |`c` varchar(255) DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        """.stripMargin)
    tidbStmt.execute("CREATE INDEX `testIndex` ON `t1` (`a`,`b`)")
    // IndexScan with Selection and with RangeFilter.
    val df1 = spark.sql("SELECT * FROM t1 where a>0 and b > 'aa'")
    val dag1 = extractDAGRequests(df1).head
    val expectation1 =
      "== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[%s]\n" +
        "   +- RowToColumnar\n" +
        "      +- TiKV FetchHandleRDD{[table: t1] IndexLookUp, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ {IndexRangeScan(Index:testindex(a,b)): { RangeFilter: [%s], Range: [%s] }, " +
        "Selection: [%s]}; " +
        "{TableRowIDScan, Selection: [%s]} }, startTs: %s}".trim
    val downgradeFilter1 = dag1.getDowngradeFilters.toArray.mkString(", ")
    val rangeFilter1 = dag1.getRangeFilter.toArray.mkString(", ")
    val selection1 = dag1.getFilters.toArray.mkString(", ")
    val myExpectationPlan1 = expectation1.format(
      downgradeFilter1,
      rangeFilter1,
      stringKeyRangeInDAG(dag1),
      selection1,
      selection1,
      dag1.getStartTs.getVersion)
    val sparkPhysicalPlan1 =
      df1.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    assert(myExpectationPlan1.equals(sparkPhysicalPlan1))

    // IndexScan without Selection and with RangeFilter.
    val df2 = spark.sql("SELECT * FROM t1 where a=0 and b > 'aa'")
    val dag2 = extractDAGRequests(df2).head
    val expection2 =
      ("== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[%s]\n" +
        "   +- RowToColumnar\n" +
        "      +- TiKV FetchHandleRDD{[table: t1] IndexLookUp, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ {IndexRangeScan(Index:testindex(a,b)): { RangeFilter: [%s], Range: [%s] }}; " +
        "{TableRowIDScan} }, startTs: %d}").trim
    val sparkPhysicalPlan2 =
      df2.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    val downgradeFilter2 = dag2.getDowngradeFilters.toArray.mkString(", ")
    val rangeFilter2 = dag2.getRangeFilter.toArray.mkString(", ")
    val myExpectationPlan2 = expection2.format(
      downgradeFilter2,
      rangeFilter2,
      stringKeyRangeInDAG(dag2),
      dag2.getStartTs.getVersion)
    assert(myExpectationPlan2.equals(sparkPhysicalPlan2))

    // CoveringIndex with Selection and with RangeFilter.
    val df3 = spark.sql("SELECT a,b FROM t1 where a>0 and b > 'aa'")
    val dag3 = extractDAGRequests(df3).head
    val expectation3 =
      ("== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiKV CoprocessorRDD{[table: t1] IndexReader, Columns: a@LONG, b@VARCHAR(255): { " +
        "IndexRangeScan(Index:testindex(a,b)): { RangeFilter: [%s], Range: [%s] }, " +
        "Selection: [%s] }, startTs: %d}").trim
    val sparkPhysicalPlan3 =
      df3.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    val rangeFilter3 = dag3.getRangeFilter.toArray.mkString(", ")
    val selection3 = dag3.getFilters.toArray.mkString(", ")
    val myExpectationPlan3 = expectation3.format(
      rangeFilter3,
      stringKeyRangeInDAG(dag3),
      selection3,
      dag3.getStartTs.getVersion)
    assert(
      myExpectationPlan3
        .equals(sparkPhysicalPlan3))

    // IndexScan with complex sql statements
    val df4 = spark.sql(
      "SELECT max(c) FROM t1 where a>0 and c > 'cc' and c < 'bb' group by c order by(c)")
    val dag4 = extractDAGRequests(df4).head
    val regionTaskExec4 =
      extractRegionTaskExecs(df4).head.verboseString(25).trim
    val rangeFilter4 = dag4.getRangeFilter.toArray.mkString(", ")
    val downgradeFilter4 = dag4.getDowngradeFilters.toArray.mkString(", ")
    val selection4 = dag4.getFilters.toArray.mkString(", ")
    var expectRegionTaskExec4 =
      ("TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[%s]")
    expectRegionTaskExec4 = expectRegionTaskExec4.format(downgradeFilter4)
    var expectDAG4 = "[table: t1] IndexLookUp, Columns: c@VARCHAR(255), a@LONG: " +
      "{ {IndexRangeScan(Index:testindex(a,b)): " +
      "{ RangeFilter: [%s], " +
      "Range: [%s] }}; " +
      "{TableRowIDScan, Selection: [%s], Aggregates: Max(c@VARCHAR(255)), First(c@VARCHAR(255)), " +
      "Group By: [c@VARCHAR(255) ASC]} }, startTs: %d"
    expectDAG4 = expectDAG4.format(
      rangeFilter4,
      stringKeyRangeInDAG(dag4),
      selection4,
      dag4.getStartTs.getVersion)
    assert(expectRegionTaskExec4.equals(regionTaskExec4))
    assert(expectDAG4.equals(dag4.toString))

    // IndexScan with complex sql statements
    val df5 = spark.sql("select sum(a) from t1 where a>0 and b > 'aa' or b<'cc' and a>0")
    val dag5 = extractDAGRequests(df5).head
    val expectation5 =
      ("[table: t1] IndexReader, Columns: a@LONG, b@VARCHAR(255): " +
        "{ IndexRangeScan(Index:testindex(a,b)): " +
        "{ RangeFilter: [%s], Range: [%s] }, Selection: [%s], Aggregates: Sum(a@LONG) }, startTs: %d").trim
    val rangeFilter5 = dag5.getRangeFilter.toArray.mkString(", ")
    val selection5 = dag5.getFilters.toArray.mkString(", ")
    val myExpectation5 = expectation5.format(
      rangeFilter5,
      stringKeyRangeInDAG(dag5),
      selection5,
      dag5.getStartTs.getVersion)
    assert(myExpectation5.equals(dag5.toString.trim))
  }

  test("test physical plan explain which table with secondary prefix index") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""
                       |CREATE TABLE `t1` (
                       |`a` BIGINT(20)  NOT NULL,
                       |`b` varchar(255) NOT NULL,
                       |`c` varchar(255) DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        """.stripMargin)
    tidbStmt.execute("CREATE INDEX `testIndex` ON `t1` (`b`(4),a)")
    // IndexScan with RangeFilter and with Selection.
    val df1 = spark.sql("SELECT * FROM t1 where a>0 and b > 'aa'")
    val dag1 = extractDAGRequests(df1).head
    val expectation1 =
      ("== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[%s]\n" +
        "   +- RowToColumnar\n" +
        "      +- TiKV FetchHandleRDD{[table: t1] IndexLookUp, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ {IndexRangeScan(Index:testindex(b,a)): { RangeFilter: [%s], Range: [%s] }}; " +
        "{TableRowIDScan, Selection: [%s]} }, startTs: %d}").trim
    val sparkPhysicalPlan1 =
      df1.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    val downgradeFilter1 = dag1.getDowngradeFilters.toArray.mkString(", ")
    val rangeFilter1 = dag1.getRangeFilter.toArray.mkString(", ")
    val selection1 = dag1.getFilters.toArray.mkString(", ")
    val myExpectationPlan1 = expectation1.format(
      downgradeFilter1,
      rangeFilter1,
      stringKeyRangeInDAG(dag1),
      selection1,
      dag1.getStartTs.getVersion)
    assert(
      myExpectationPlan1
        .equals(sparkPhysicalPlan1))

    // IndexScan with RangeFilter and without Selection.
    val df2 = spark.sql("SELECT * FROM t1 where b > 'aa'")
    val dag2 = extractDAGRequests(df2).head
    val expectation2 =
      ("== Physical Plan ==\n" +
        "*(1) ColumnarToRow\n" +
        "+- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[%s]\n" +
        "   +- RowToColumnar\n" +
        "      +- TiKV FetchHandleRDD{[table: t1] IndexLookUp, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): " +
        "{ {IndexRangeScan(Index:testindex(b,a)): { RangeFilter: [%s], Range: [%s] }}; " +
        "{TableRowIDScan, Selection: [%s]} }, startTs: %d}").trim
    val sparkPhysicalPlan2 =
      df2.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name)).trim
    val downgradeFilter2 = dag2.getDowngradeFilters.toArray.mkString(", ")
    val rangeFilter2 = dag2.getRangeFilter.toArray.mkString(", ")
    val selection2 = dag2.getFilters.toArray.mkString(", ")
    val myExpectationPlan2 = expectation2.format(
      downgradeFilter2,
      rangeFilter2,
      stringKeyRangeInDAG(dag2),
      selection2,
      dag2.getStartTs.getVersion)
    assert(
      myExpectationPlan2
        .equals(sparkPhysicalPlan2))

    // IndexScan with complex sql statements
    val df3 = spark.sql("SELECT sum(a) FROM t1 where  b > 'cc' or b < 'bb' and a>0 limit(10)")
    val dag3 = extractDAGRequests(df3).head
    val regionTaskExec3 =
      extractRegionTaskExecs(df3).head.verboseString(25).trim
    val downgradeFilter3 = dag3.getDowngradeFilters.toArray.mkString(", ")
    val selection3 = dag3.getFilters.toArray.mkString(", ")
    var expectRegionTaskExec3 =
      ("TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[%s]")
    expectRegionTaskExec3 = expectRegionTaskExec3.format(downgradeFilter3)
    var expectDAG3 = "[table: t1] IndexLookUp, Columns: b@VARCHAR(255), a@LONG: " +
      "{ {IndexRangeScan(Index:testindex(b,a)): { RangeFilter: [], Range: [%s] }}; " +
      "{TableRowIDScan, Selection: [%s], Aggregates: Sum(a@LONG)} }, startTs: %d"
    expectDAG3 =
      expectDAG3.format(stringKeyRangeInDAG(dag3), selection3, dag3.getStartTs.getVersion)
    assert(expectRegionTaskExec3.equals(regionTaskExec3))
    assert(expectDAG3.equals(dag3.toString))
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
    checkIsIndexLookUp(df, "t")
    checkIndex(df, "artical_id")
  }

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

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists t1")
      tidbStmt.execute("drop table if exists test1")
      tidbStmt.execute("drop table if exists test2")
      tidbStmt.execute("drop table if exists test3")
    } finally {
      super.afterAll()
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
}
