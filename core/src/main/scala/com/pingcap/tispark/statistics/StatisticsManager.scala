package com.pingcap.tispark.statistics

import com.pingcap.tidb.tipb.AnalyzeColumnsResp
import com.pingcap.tikv.TiSession
import com.pingcap.tikv.key.RowKey
import com.pingcap.tikv.meta.{TiAnalyzeRequest, TiTableInfo}
import com.pingcap.tikv.util.KeyRangeUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StatisticsManager(tiSession: TiSession) {
  private val snapshot = tiSession.createSnapshot()

  def analyze(table: TiTableInfo): List[AnalyzeColumnsResp] = {
    val ar = new TiAnalyzeRequest
    ar.addColumnInfos(table.getColumns.asScala.map(_.toProto(table)))
    val start = RowKey.createMin(table.getId)
    val end = RowKey.createBeyondMax(table.getId)
    ar.addRange(KeyRangeUtils.makeCoprocRange(start.toByteString, end.toByteString))
    snapshot.analyzeColumn(ar)
  }
}
