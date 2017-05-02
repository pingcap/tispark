package com.pingcap.tispark

import org.apache.spark.Partition


class TiPartition(idx: Int) extends Partition {
  override def index: Int = idx
}
