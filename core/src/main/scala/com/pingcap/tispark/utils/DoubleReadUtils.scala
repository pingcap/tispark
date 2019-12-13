package com.pingcap.tispark.utils

import java.util

import com.google.common.collect.ImmutableList
import com.pingcap.tikv.region.RegionManager
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import gnu.trove.list.array.TLongArrayList

object DoubleReadUtils {

  // After `splitAndSortHandlesByRegion`, ranges in the task are arranged in order
  def generateIndexTasks(handles: TLongArrayList,
                         regionManager: RegionManager,
                         ids: java.util.List[java.lang.Long]): util.List[RegionTask] = {
    val indexTasks: util.List[RegionTask] = new util.ArrayList[RegionTask]()
    indexTasks.addAll(
      RangeSplitter
        .newSplitter(regionManager)
        .splitAndSortHandlesByRegion(ids, handles)
    )
    indexTasks
  }

  // After `splitAndSortHandlesByRegion`, ranges in the task are arranged in order
  def generateIndexTasks(handles: TLongArrayList,
                         regionManager: RegionManager,
                         id: java.lang.Long): util.List[RegionTask] = {
    generateIndexTasks(handles, regionManager, ImmutableList.of(id))
  }

}
