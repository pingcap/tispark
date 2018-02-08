package com.pingcap.tispark.statistics

import com.pingcap.tidb.tipb.{AnalyzeColumnsResp, AnalyzeIndexResp}
import com.pingcap.tikv.TiSession
import com.pingcap.tikv.expression.{ColumnRef, ComparisonBinaryExpression, Constant}
import com.pingcap.tikv.key.{IndexKey, Key, RowKey}
import com.pingcap.tikv.kvproto.Coprocessor
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType
import com.pingcap.tikv.meta.{TiAnalyzeRequest, TiDAGRequest, TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.row.Row
import com.pingcap.tikv.util.KeyRangeUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class StatisticsManager(tiSession: TiSession) {
  private val snapshot = tiSession.createSnapshot()
  private val tableHistMap = collection.mutable.HashMap[TiTableInfo, List[AnalyzeColumnsResp]]()
  private val indexHistMap =
    collection.mutable.HashMap[(TiTableInfo, TiIndexInfo), List[AnalyzeIndexResp]]()

  def analyze(table: TiTableInfo): Unit = {
    val req = new TiDAGRequest(PushDownType.NORMAL)
    req.setTableInfo(table)
    val start = RowKey.createMin(table.getId)
    val end = RowKey.createBeyondMax(table.getId)
    val ranges = mutable.ArrayBuffer[Coprocessor.KeyRange]()
    ranges += KeyRangeUtils.makeCoprocRange(start.toByteString, end.toByteString)
    req.addFilter(ComparisonBinaryExpression.equal(ColumnRef.create("table_id", table), Constant.create("table_id")))
    req.addRanges(ranges)
    val rows = snapshot.tableRead(req)
    
    val colReq = new TiAnalyzeRequest
    // Fetch table column info
    colReq.addColumnInfos(table.getColumns.asScala.map(_.toProto(table)))
    tableHistMap(table) = snapshot.analyzeColumn(colReq).toList
    // Fetch all index histogram info
    table.getIndices.foreach((index: TiIndexInfo) => {
      val idxReq = new TiAnalyzeRequest
      idxReq.setIndexInfo(index)
      val start = IndexKey.toIndexKey(table.getId, index.getId, Key.MIN)
      val end = IndexKey.toIndexKey(table.getId, index.getId, Key.MAX)
      idxReq.addRange(KeyRangeUtils.makeCoprocRange(start.toByteString, end.toByteString))
      indexHistMap((table, index)) = snapshot.analyzeIndex(idxReq).toList
    })
  }
}
