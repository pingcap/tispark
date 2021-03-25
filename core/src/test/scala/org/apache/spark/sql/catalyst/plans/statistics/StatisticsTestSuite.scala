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

package org.apache.spark.sql.catalyst.plans.statistics

import com.google.common.collect.ImmutableList
import com.pingcap.tikv.expression.ComparisonBinaryExpression.{
  equal,
  greaterThan,
  lessEqual,
  lessThan
}
import com.pingcap.tikv.expression.{ColumnRef, Constant, Expression, LogicalBinaryExpression}
import com.pingcap.tikv.meta.{TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.predicates.PredicateUtils.expressionToIndexRanges
import com.pingcap.tikv.predicates.TiKVScanAnalyzer
import com.pingcap.tikv.types.IntegerType
import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.sql.catalyst.plans.BasePlanTest
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConverters._

class StatisticsTestSuite extends BasePlanTest {
  private val tableScanCases = Set("select * from full_data_type_table_idx where id_dt = 2333")
  private val indexScanCases = Map(
    // double read case
    "select tp_bigint, tp_real from full_data_type_table_idx where tp_int = 2333" -> "idx_tp_int",
    "select * from full_data_type_table_idx where tp_int = 2333" -> "idx_tp_int")
  private val coveringIndexScanCases = Map(
    // cover index case
    "select id_dt from full_data_type_table_idx where tp_int = 2333" -> "idx_tp_int",
    "select tp_int from full_data_type_table_idx where tp_bigint < 10 and tp_int < 40" -> "idx_tp_int_tp_bigint",
    "select tp_int from full_data_type_table_idx where tp_bigint < -4511898209778166952 and tp_int < 40" -> "idx_tp_bigint_tp_int")
  protected var fDataTbl: TiTableInfo = _
  protected var fDataIdxTbl: TiTableInfo = _

  test("Test fixed table size estimation") {
    tidbStmt.execute("DROP TABLE IF EXISTS `tb_fixed_float`")
    tidbStmt.execute("DROP TABLE IF EXISTS `tb_fixed_int`")
    tidbStmt.execute("DROP TABLE IF EXISTS `tb_fixed_time`")
    tidbStmt.execute("""CREATE TABLE `tb_fixed_float` (
        |  `c1` FLOAT NOT NULL,
        |  `c2` DOUBLE NOT NULL,
        |  `c3` DECIMAL(5,2) NOT NULL
        |)""".stripMargin)
    tidbStmt.execute(
      "insert into `tb_fixed_float` values(1.4, 1.2, 4.4), (1.2, 2.0, 55.22), (3.14, 1.0, 4.11), (2.4, 2.5, 9.99)")
    tidbStmt.execute("""CREATE TABLE `tb_fixed_int` (
        |  `c1` tinyint NOT NULL,
        |  `c2` smallint NOT NULL,
        |  `c3` bigint NOT NULL,
        |  `c4` int NOT NULL,
        |  `c5` mediumint NOT NULL
        |)""".stripMargin)
    tidbStmt.execute(
      "insert into `tb_fixed_int` values(1, 1, 1, 2, 1), (1, 2, 1, 1, 1), (2, 1, 3, 2, 1), (2, 2, 2, 1, 1)")
    tidbStmt.execute("""CREATE TABLE `tb_fixed_time` (
        |  `c1` YEAR NOT NULL,
        |  `c2` DATE NOT NULL,
        |  `c3` TIMESTAMP NOT NULL,
        |  `c4` DATETIME NOT NULL,
        |  `c5` TIME NOT NULL
        |)""".stripMargin)
    tidbStmt.execute(
      "insert into `tb_fixed_time` values(2018, '1997-11-22', '1995-11-06 00:00:00', '1970-09-07 00:00:00', '12:00:00')")
    tidbStmt.execute(
      "insert into `tb_fixed_time` values(2011, '1997-11-22', '1995-11-06 00:00:00', '1970-09-07 00:00:00', '12:00:00')")
    tidbStmt.execute("analyze table `tb_fixed_int`")
    tidbStmt.execute("analyze table `tb_fixed_float`")
    tidbStmt.execute("analyze table `tb_fixed_time`")
    refreshConnections()
    val tbFixedInt = ti.meta.getTable(s"${dbPrefix}tispark_test", "tb_fixed_int").get
    val tbFixedFloat = ti.meta.getTable(s"${dbPrefix}tispark_test", "tb_fixed_float").get
    val tbFixedTime = ti.meta.getTable(s"${dbPrefix}tispark_test", "tb_fixed_time").get
    val intBytes = StatisticsManager.estimateTableSize(tbFixedInt)
    val floatBytes = StatisticsManager.estimateTableSize(tbFixedFloat)
    val timeBytes = StatisticsManager.estimateTableSize(tbFixedTime)
    assert(intBytes >= 18 * 4)
    assert(floatBytes >= 22 * 4)
    assert(timeBytes >= 19 * 2)
  }

  test("select count(1) from full_data_type_table_idx where tp_int = 2006469139 or tp_int < 0") {
    val indexes = fDataIdxTbl.getIndices.asScala
    val idx = indexes.filter(_.getIndexColumns.asScala.exists(_.matchName("tp_int"))).head

    val eq1: Expression =
      equal(ColumnRef.create("tp_int", fDataIdxTbl), Constant.create(2006469139, IntegerType.INT))
    val eq2: Expression =
      lessEqual(ColumnRef.create("tp_int", fDataIdxTbl), Constant.create(0, IntegerType.INT))
    val or: Expression = LogicalBinaryExpression.or(eq1, eq2)

    val expressions = ImmutableList.of(or).asScala
    testSelectRowCount(expressions, idx, 46)
  }

  test(
    "select tp_int from full_data_type_table_idx where tp_int < 5390653 and tp_int > -46759812") {
    val indexes = fDataIdxTbl.getIndices.asScala
    val idx = indexes.filter(_.getIndexColumns.asScala.exists(_.matchName("tp_int"))).head

    val le1: Expression =
      lessThan(ColumnRef.create("tp_int", fDataIdxTbl), Constant.create(5390653, IntegerType.INT))
    val gt: Expression =
      greaterThan(
        ColumnRef.create("tp_int", fDataIdxTbl),
        Constant.create(-46759812, IntegerType.INT))
    val and: Expression = LogicalBinaryExpression.and(le1, gt)

    val expressions = ImmutableList.of(and).asScala
    testSelectRowCount(expressions, idx, 5)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    initTable()
  }

  private def initTable(): Unit = {
    fDataTbl = getTableInfo("tispark_test", "full_data_type_table")
    fDataIdxTbl = getTableInfo("tispark_test", "full_data_type_table_idx")
    StatisticsManager.loadStatisticsInfo(fDataTbl)
    StatisticsManager.loadStatisticsInfo(fDataIdxTbl)
  }

  def testSelectRowCount(
      expressions: Seq[Expression],
      idx: TiIndexInfo,
      expectedCount: Long): Unit = {
    val result = TiKVScanAnalyzer.extractConditions(expressions.asJava, fDataIdxTbl, idx)
    val irs =
      expressionToIndexRanges(
        result.getPointPredicates,
        result.getRangePredicate,
        fDataIdxTbl,
        idx)
    initTable()
    val tblStatistics = StatisticsManager.getTableStatistics(fDataIdxTbl.getId)
    val idxStatistics = tblStatistics.getIndexHistMap.get(idx.getId)
    val rc = idxStatistics.getRowCount(irs).toLong
    assert(rc == expectedCount)
  }

  tableScanCases.foreach { query =>
    {
      val tableName = "full_data_type_table_idx"
      test(query) {
        if (isEnableAlterPrimaryKey || supportClusteredIndex) {
          cancel()
        }
        val df = spark.sql(query)
        checkIsTableScan(df, tableName)
      }
    }
  }

  indexScanCases.foreach {
    case (query, idxName) =>
      val tableName = "full_data_type_table_idx"
      test(query) {
        val df = spark.sql(query)
        checkIsIndexScan(df, tableName)
        checkIndex(df, idxName)
      }
  }

  coveringIndexScanCases.foreach {
    case (query, idxName) =>
      val tableName = "full_data_type_table_idx"
      test(query) {
        if (isEnableAlterPrimaryKey || supportClusteredIndex) {
          cancel()
        }
        val df = spark.sql(query)
        checkIsCoveringIndexScan(df, tableName)
        checkIndex(df, idxName)
      }
  }

  ignore("baseline test") {
    val tableName = "full_data_type_table_idx"
    val df = spark.sql(s"select tp_bigint from $tableName where tp_tinyint = 0 and tp_int < 0")

    checkIndex(df, "idx_tp_tinyint_tp_int")

    assertThrows[TestFailedException] {
      checkIsTableScan(df, tableName)
    }

    checkIsIndexScan(df, tableName)

    assertThrows[TestFailedException] {
      checkIsCoveringIndexScan(df, tableName)
    }

    checkEstimatedRowCount(df, tableName, 2)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("DROP TABLE IF EXISTS `tb_fixed_float`")
      tidbStmt.execute("DROP TABLE IF EXISTS `tb_fixed_int`")
      tidbStmt.execute("DROP TABLE IF EXISTS `tb_fixed_time`")
    } finally {
      super.afterAll()
    }
}
