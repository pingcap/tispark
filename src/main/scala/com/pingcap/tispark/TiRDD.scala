package com.pingcap.tispark

import com.google.proto4pingcap.ByteString
import com.pingcap.tidb.tipb.SelectRequest
import com.pingcap.tikv.catalog.Catalog
import com.pingcap.tikv.meta.{TiDBInfo, TiRange, TiTableInfo}
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.{Snapshot, TiCluster, TiConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._


class TiRDD(selectRequestInBytes: ByteString, ranges: List[TiRange[java.lang.Long]], sc: SparkContext, options: TiOptions)
  extends RDD[Row](sc, Nil) {

  @transient var tiConf: TiConfiguration = null
  @transient var cluster: TiCluster = null
  @transient var catalog: Catalog = null
  @transient var database: TiDBInfo = null
  @transient var table: TiTableInfo = null
  @transient var snapshot: Snapshot = null
  @transient var selectReq: SelectRequest = null

  init()

  def init(): Unit = {
    tiConf = TiConfiguration.createDefault(options.addresses.asJava)
    cluster = TiCluster.getCluster(tiConf)
    catalog = cluster.getCatalog
    database = catalog.getDatabase(options.databaseName)
    table = catalog.getTable(database, options.tableName)
    snapshot = cluster.createSnapshot()
    selectReq = SelectRequest.parseFrom(selectRequestInBytes)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    init()
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
