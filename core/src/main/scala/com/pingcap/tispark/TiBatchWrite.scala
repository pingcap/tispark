/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark

import com.pingcap.tikv.TiBatchWriteUtils
import com.pingcap.tikv.codec.KeyUtils
import com.pingcap.tikv.exception.TableNotExistException
import com.pingcap.tikv.key.{Key, RowKey}
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.util.KeyRangeUtils
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.TiContext
import org.slf4j.LoggerFactory

/**
  * An ugly implementation of batch write framework, which will be
  * replaced by spark api.
  */
object TiBatchWrite {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType

  def writeToTiDB(rdd: RDD[SparkRow], tableRef: TiTableReference, tiContext: TiContext) {
    // TODO: region pre-split
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-69

    // TODO: lock table
    // pending: https://internal.pingcap.net/jira/browse/TIDB-1628

    // shuffle data in same task which belong to same region
    val shuffledRDD = {
      val tiKVRowRDD = rdd.map(sparkRow2TiKVRow)
      shuffleKeyToSameRegion(tiKVRowRDD, tableRef, tiContext)
    }

    // TODO: resolve lock
    // why here? can we do resolve after receiving an error during writing data?

    // take one row as primary key
    val (primaryKey: SerializableKey, primaryRow: TiRow) = {
      val takeOne = shuffledRDD.take(1)
      if (takeOne.length == 0) {
        logger.warn("there is no data in source rdd")
        return
      } else {
        takeOne(0)
      }
    }
    logger.info(s"primary key: $primaryKey primary row: $primaryRow")

    // TODO: get timestamp as start_ts

    // TODO: driver primary pre-write

    // TODO: executors secondary pre-write

    // TODO: driver primary commit

    // TODO: executors secondary commit
  }

  @throws(classOf[TableNotExistException])
  private def shuffleKeyToSameRegion(rdd: RDD[TiRow],
                                  tableRef: TiTableReference,
                                  tiContext: TiContext): RDD[(SerializableKey, TiRow)] = {
    val regions = getRegions(tableRef, tiContext)
    val tiRegionPartitioner = new TiRegionPartitioner(regions)
    val databaseName = tableRef.databaseName
    val tableName = tableRef.tableName
    val session = tiContext.tiSession
    val table = session.getCatalog.getTable(databaseName, tableName)
    val tableId = table.getId

    logger.info(
      s"find ${regions.size} regions in database: $databaseName table: $tableName tableId: $tableId"
    )

    rdd
      .map(row => (tiKVRow2Key(row, tableId), row))
      .groupByKey(tiRegionPartitioner)
      .map {
        case (key, iterable) =>
          // remove duplicate rows if key equals
          (key, iterable.head)
      }
  }

  @throws(classOf[TableNotExistException])
  private def getRegions(tableRef: TiTableReference, tiContext: TiContext): List[TiRegion] = {
    import scala.collection.JavaConversions._
    TiBatchWriteUtils.getRegionsByTable(tiContext.tiSession, tableRef.databaseName, tableRef.tableName).toList
  }

  private def tiKVRow2Key(row: TiRow, tableId: Long): SerializableKey = {
    // TODO: how to get primary key if exists || auto generate a primary key if does not exists
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-70
    val handle = row.getLong(0)

    val rowKey = RowKey.toRowKey(tableId, handle)
    new SerializableKey(rowKey.getBytes)
  }

  private def sparkRow2TiKVRow(sparkRow: SparkRow): TiRow = {
    val fieldCount = sparkRow.size
    val tiRow = ObjectRowImpl.create(fieldCount)
    for (i <- 0 until fieldCount) {
      val data = sparkRow.get(i)
      val sparkDataType = sparkRow.schema(i).dataType
      val tiDataType = TiUtils.fromSparkType(sparkDataType)
      tiRow.set(i, tiDataType, data)
    }
    tiRow
  }
}

class TiRegionPartitioner(regions: List[TiRegion]) extends Partitioner {
  override def numPartitions: Int = regions.length

  override def getPartition(key: Any): Int = {
    val serializableKey = key.asInstanceOf[SerializableKey]
    val rawKey = Key.toRawKey(serializableKey.bytes)

    regions.indices.foreach { i =>
      val region = regions(i)
      val range = KeyRangeUtils.makeRange(region.getStartKey, region.getEndKey)

      if (range.contains(rawKey)) {
        return i
      }
    }

    0
  }
}

class SerializableKey(val bytes: Array[Byte]) extends Serializable {
  override def toString: String =
    KeyUtils.formatBytes(bytes)
}
