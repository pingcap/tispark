package com.pingcap.tispark

import com.google.proto4pingcap.ByteString
import com.pingcap.tikv.grpc.Metapb.{Region, Store}
import com.pingcap.tikv.meta.TiRange
import org.apache.spark.Partition


class TiPartition(idx: Int, val region: Region, val store: Store, val tiRange: TiRange[ByteString]) extends Partition {
  override def index: Int = idx
}
