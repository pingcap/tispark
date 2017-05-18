package com.pingcap.tispark

import com.google.proto4pingcap.ByteString
import com.pingcap.tidb.tipb.SelectRequest
import com.pingcap.tikv.meta.TiRange
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.{Snapshot, TiCluster, TiConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._


class TiRDD(selectRequestInBytes: ByteString, ranges: List[TiRange[java.lang.Long]], sc: SparkContext, options: TiOptions)
  extends RDD[Row](sc, Nil) {

  @transient val tiConf = TiConfiguration.createDefault(options.addresses.asJava)
  @transient val cluster = TiCluster.getCluster(tiConf)
  @transient val catalog = cluster.getCatalog
  @transient val database = catalog.getDatabase(options.databaseName)
  @transient val table = catalog.getTable(database, options.tableName)
  @transient val snapshot = cluster.createSnapshot()
  @transient val selectReq = SelectRequest.parseFrom(selectRequestInBytes)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    context.addTaskCompletionListener{ _ => cluster.close() }

    val tiPartition = split.asInstanceOf[TiPartition]
    val iterator = snapshot.select(selectReq, tiPartition.region, tiPartition.store, tiPartition.tiRange)
    def toSparkRow(row: com.pingcap.tikv.meta.Row): Row = {
      val rowArray = new Array[Any](row.fieldCount())
      for (i <- 0 until row.fieldCount()) {
        rowArray(i) = row.get(i, table.getColumns.get(i).getType)
      }
      Row.fromSeq(rowArray)
    }

    override def hasNext: Boolean = {
      iterator.hasNext
    }

    override def next(): Row = {
      toSparkRow(iterator.next)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val keyRanges = Snapshot.convertHandleRangeToKeyRange(table, ranges.asJava)
    val keyWithRegionRanges = RangeSplitter.newSplitter(cluster.getRegionManager)
                 .splitRangeByRegion(keyRanges)
    keyWithRegionRanges.asScala.zipWithIndex.map{
      case (keyRegionPair, index) => new TiPartition(index,
                                            keyRegionPair.first.first, /* Region */
                                            keyRegionPair.first.second, /* Store */
                                            keyRegionPair.second) /* Range */
    }.toArray
  }
}
