package com.pingcap.tispark.v2

import com.pingcap.tispark.TiTableReference
import org.apache.spark.sql.connector.read.{Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

import java.util.OptionalLong

case class TiDBTableScan(
     tableRef: TiTableReference,
     schema: StructType)
  extends Scan
    with SupportsReportStatistics {

  override def readSchema(): StructType = schema

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val size = tableRef.sizeInBytes
        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()

    }
  }
}
