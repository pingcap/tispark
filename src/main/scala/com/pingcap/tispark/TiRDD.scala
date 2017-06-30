package com.pingcap.tispark

import com.pingcap.tikv._
import com.pingcap.tikv.catalog.Catalog
import com.pingcap.tikv.meta.{TiDBInfo, TiSelectRequest, TiTableInfo}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.RangeSplitter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._


class TiRDD(selReq: TiSelectRequest, sc: SparkContext, options: TiOptions)
  extends RDD[Row](sc, Nil) {

  type TiRow = com.pingcap.tikv.row.Row

  @transient var tiConf: TiConfiguration = _
  @transient var cluster: TiCluster = _
  @transient var catalog: Catalog = _
  @transient var database: TiDBInfo = _
  @transient var table: TiTableInfo = _
  @transient var snapshot: Snapshot = _
  @transient var selectReq: TiSelectRequest = _
  @transient var fieldsType:List[DataType] = _
  @transient var rt: RowTransformer = _
  @transient var finalTypes: List[DataType] = _

  init()

  def init(): Unit = {
    tiConf = TiConfiguration.createDefault(options.addresses)
    cluster = TiCluster.getCluster(tiConf)
    catalog = cluster.getCatalog
    database = catalog.getDatabase(options.databaseName)
    table = catalog.getTable(database, options.tableName)
    selectReq = selReq
    snapshot = cluster.createSnapshot()
    selectReq.bind
    val schemaInferer = SchemaInfer.create(selectReq)
    fieldsType = schemaInferer.getTypes.toList
    rt = schemaInferer.getRowTransformer
    finalTypes = rt.getTypes.toList
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    init()
    context.addTaskCompletionListener{ _ => cluster.close() }

    // bypass, sum return a long type
    val tiPartition = split.asInstanceOf[TiPartition]
    val iterator = snapshot.select(selectReq, split.asInstanceOf[TiPartition].task)

    def toSparkRow(row: TiRow): Row = {
      val transRow = rt.transform(row)
      val rowArray = new Array[Any](rt.getTypes.size)

      for (i <- 0 until transRow.fieldCount) {
        rowArray(i) = transRow.get(i, finalTypes(i))
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
    val keyWithRegionTasks = RangeSplitter.newSplitter(cluster.getRegionManager)
                 .splitRangeByRegion(selReq.getRanges)

    keyWithRegionTasks.zipWithIndex.map{
      case (task, index) => new TiPartition(index, task)
    }.toArray
  }
}
