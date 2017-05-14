package com.pingcap.tispark

import com.google.proto4pingcap.ByteString
import com.pingcap.tikv.meta.TiRange
import com.pingcap.tikv.{TiCluster, TiConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._


class TiRDD(selectRequestInBytes: ByteString, sc: SparkContext, options: TiOptions)
  extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    val tiConf = TiConfiguration.createDefault(options.addresses.asJava)
    val cluster = TiCluster.getCluster(tiConf)

    context.addTaskCompletionListener{ _ => cluster.close() }

    val catalog = cluster.getCatalog
    val database = catalog.getDatabase(options.databaseName)
    val table = catalog.getTable(database, options.tableName)
    val snapshot = cluster.createSnapshot()

    val iterator = snapshot.newSelect(table)
                           .addRange(TiRange.create[java.lang.Long](0L, Long.MaxValue))
                           .doSelect()
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
    Array(new TiPartition(0))
  }
}
