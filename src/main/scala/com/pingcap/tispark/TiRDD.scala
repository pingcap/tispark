package com.pingcap.tispark

import java.util

import com.google.proto4pingcap.ByteString
import com.pingcap.tidb.tipb.{ExprType, SelectRequest}
import com.pingcap.tikv.`type`.{DecimalType, FieldType, LongType, StringType}
import com.pingcap.tikv.catalog.Catalog
import com.pingcap.tikv.meta.{TiDBInfo, TiRange, TiTableInfo}
import com.pingcap.tikv.operation.TypeInferer
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._


class TiRDD(selectRequestInBytes: ByteString, ranges: List[TiRange[java.lang.Long]], sc: SparkContext, options: TiOptions)
  extends RDD[Row](sc, Nil) {

  @transient var tiConf: TiConfiguration = _
  @transient var cluster: TiCluster = _
  @transient var catalog: Catalog = _
  @transient var database: TiDBInfo = _
  @transient var table: TiTableInfo = _
  @transient var snapshot: Snapshot = _
  @transient var selectReq: SelectRequest = _
  @transient var fieldsType:Array[FieldType] = _

  init()

  def init(): Unit = {
    tiConf = TiConfiguration.createDefault(options.addresses.asJava)
    cluster = TiCluster.getCluster(tiConf)
    catalog = cluster.getCatalog
    database = catalog.getDatabase(options.databaseName)
    table = catalog.getTable(database, options.tableName)
    snapshot = cluster.createSnapshot()
    selectReq = SelectRequest.parseFrom(selectRequestInBytes)
    fieldsType = TypeInferer.toFieldTypes(selectReq)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    init()
    context.addTaskCompletionListener{ _ => cluster.close() }

    // byPass, sum return a long type
    val tiPartition = split.asInstanceOf[TiPartition]
    val iterator = snapshot.select(selectReq, tiPartition.region, tiPartition.store, tiPartition.tiRange)
    def toSparkRow(row: com.pingcap.tikv.meta.Row): Row = {
      //TODO byPass for sum
      val rowArray = new Array[Any](row.fieldCount())
      // if sql does not have group by, simple skip "SingledGroup"
      if (selectReq.getGroupByCount == 0 && selectReq.getAggregatesCount > 0) {
        for (i <- 1 until row.fieldCount()) {
          // TODO Array[Pair] sourceType -> outputType
          if (!fieldsType(i).isInstanceOf[StringType]) {
            if (selectReq.getAggregatesCount == 1) {
              if (selectReq.getAggregates(0).getTp == ExprType.Sum) {
                val value = row.getDecimal(i)
                rowArray(0) = Math.round(value)
              } else {
                rowArray(0) = row.get(i, fieldsType(i))
              }
            } else {
               if (selectReq.getAggregates(i - 1).getTp == ExprType.Sum) {
                val value = row.getDecimal(i)
                rowArray(0) = Math.round(value)
              } else {
                rowArray(1) = row.get(i, fieldsType(i))
              }
            }
          }
        }
      } else {
        for (i <- 0 until row.fieldCount()) {
          rowArray(i) = row.get(i, fieldsType(i))
        }
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
