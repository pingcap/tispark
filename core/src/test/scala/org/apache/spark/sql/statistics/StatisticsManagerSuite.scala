package org.apache.spark.sql.statistics

import java.util
import java.util.List

import com.google.common.collect.ImmutableList
import com.pingcap.tikv.expression.ComparisonBinaryExpression.{equal, lessEqual}
import com.pingcap.tikv.expression._
import com.pingcap.tikv.key.TypedKey
import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tikv.predicates.PredicateUtils.expressionToIndexRanges
import com.pingcap.tikv.predicates.SelectivityCalculator.CalculationContext
import com.pingcap.tikv.predicates.{IndexRange, ScanAnalyzer, ScanSpec, SelectivityCalculator}
import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.sql.BaseTiSparkSuite

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StatisticsManagerSuite extends BaseTiSparkSuite {
  protected var fDataTbl:TiTableInfo = _
  protected var fDataIdxTbl:TiTableInfo = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    loadStatistics()
  }

  private def loadStatistics():Unit = {
    fDataIdxTbl = ti.meta.getTable("tispark_test", "full_data_type_table_idx").get
    fDataTbl = ti.meta.getTable("tispark_test", "full_data_type_table").get
    ti.statisticsManager.tableStatsFromStorage(fDataIdxTbl)
    ti.statisticsManager.tableStatsFromStorage(fDataTbl)
  }

  test("TP_INT statistic count") {
    val indexes = fDataIdxTbl.getIndices
    val idx = indexes.filter(_.getIndexColumns.asScala.exists(_.matchName("tp_int"))).head
    val eq1: Expression = equal(ColumnRef.create("tp_int", fDataIdxTbl), Constant.create(2006469139))
    val eq2: Expression = lessEqual(ColumnRef.create("tp_int", fDataIdxTbl), Constant.create(0))
    val or: Expression = LogicalBinaryExpression.or(eq1, eq2)

    val exps = ImmutableList.of(or)
    val result = ScanAnalyzer.extractConditions(exps, fDataIdxTbl, idx)
    val irs = expressionToIndexRanges(result.getPointPredicates, result.getRangePredicate)
    val tblStatistics = StatisticsManager.getInstance().getTableStatistics(fDataIdxTbl.getId)
    val idxStatistics = tblStatistics.getIndexHistMap.get(idx.getId)
    val rc = idxStatistics.getRowCount(irs).toLong
    assert(rc == 46)
  }

  test("Index scan test cases") {
    spark.sql("select tp_int from full_data_type_table_idx where tp_bigint < 10").explain()
  }
}
