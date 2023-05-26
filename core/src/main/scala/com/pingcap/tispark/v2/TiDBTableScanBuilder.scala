package com.pingcap.tispark.v2

import com.pingcap.tispark.TiTableReference
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

class TiDBTableScanBuilder(
    tableRef: TiTableReference,
    schema: StructType
    ) extends ScanBuilder {
  override def build(): Scan = {
    TiDBTableScan(tableRef,schema)
  }
}
