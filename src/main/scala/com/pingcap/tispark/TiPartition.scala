package com.pingcap.tispark

import com.pingcap.tikv.util.RangeSplitter.RegionTask
import org.apache.spark.Partition


class TiPartition(idx: Int, val task: RegionTask) extends Partition {
  override def index: Int = idx
}
