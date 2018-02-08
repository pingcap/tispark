package com.pingcap.tispark.statistics

import com.pingcap.tidb.tipb.{AnalyzeColumnsResp, AnalyzeIndexResp}
import com.pingcap.tikv.TiSession
import com.pingcap.tikv.key.{IndexKey, Key, RowKey}
import com.pingcap.tikv.meta.{TiAnalyzeRequest, TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.util.KeyRangeUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StatisticsManager(tiSession: TiSession) {
  private val snapshot = tiSession.createSnapshot()
  private val tableHistMap = collection.mutable.HashMap[TiTableInfo, List[AnalyzeColumnsResp]]()
  private val indexHistMap =
    collection.mutable.HashMap[(TiTableInfo, TiIndexInfo), List[AnalyzeIndexResp]]()

  def analyze(table: TiTableInfo): Unit = {
    val colReq = new TiAnalyzeRequest
    // Fetch table column info
    colReq.addColumnInfos(table.getColumns.asScala.map(_.toProto(table)))
    val start = RowKey.createMin(table.getId)
    val end = RowKey.createBeyondMax(table.getId)
    colReq.addRange(KeyRangeUtils.makeCoprocRange(start.toByteString, end.toByteString))
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
