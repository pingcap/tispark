/*
 * Copyright 2019 PingCAP, Inc.
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

import com.pingcap.tikv.codec.{CodecDataOutput, KeyUtils, TableCodec}
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.{Key, RowKey}
import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer, KeyRangeUtils}
import com.pingcap.tikv.{TiBatchWriteUtils, _}
import gnu.trove.list.array.TLongArrayList
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
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

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def writeToTiDB(rdd: RDD[SparkRow], tableRef: TiTableReference, tiContext: TiContext) {
    // initialize
    val tiConf = tiContext.tiConf
    val tiSession = tiContext.tiSession
    val kvClient = tiSession.createTxnClient()
    val (tiTableInfo, colDataTypes, colIds) = getTableInfo(tableRef, tiContext)

    // TODO: region pre-split
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-69

    // TODO: lock table
    // pending: https://internal.pingcap.net/jira/browse/TIDB-1628

    // shuffle data in same task which belong to same region
    val shuffledRDD = {
      val tiKVRowRDD = rdd.map(sparkRow2TiKVRow)
      shuffleKeyToSameRegion(tiKVRowRDD, tableRef, tiTableInfo, tiContext)
    }

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

    // filter primary key
    val finalWriteRDD = shuffledRDD.filter {
      case (key, _) => !key.equals(primaryKey)
    }

    // get timestamp as start_ts
    val startTs = kvClient.getTimestamp.getVersion
    logger.info(s"startTS: $startTs")

    // driver primary pre-write
    val ti2PCClient = new TiBatchWrite2PCClient(kvClient, startTs)
    val prewritePrimaryBackoff =
      ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)
    val encodedRow = encodeTiRow(primaryRow, colDataTypes, colIds)
    ti2PCClient.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey.bytes, encodedRow)

    // executors secondary pre-write
    finalWriteRDD.foreachPartition { iterator =>
      val tiSessionOnExecutor = new TiSession(tiConf)
      val kvClientOnExecutor = tiSessionOnExecutor.createTxnClient()
      val ti2PCClientOnExecutor = new TiBatchWrite2PCClient(kvClientOnExecutor, startTs)
      val prewriteSecondaryBackoff =
        ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)

      import scala.collection.JavaConverters._
      val pairs = iterator.map {
        case (key, row) =>
          new TiBatchWrite2PCClient.BytePairWrapper(
            key.bytes,
            encodeTiRow(row, colDataTypes, colIds)
          )
      }.asJava

      ti2PCClientOnExecutor.prewriteSecondaryKeys(prewriteSecondaryBackoff, primaryKey.bytes, pairs)
    }

    // driver primary commit
    val commitTs = kvClient.getTimestamp.getVersion
    // check commitTS
    if (commitTs <= startTs) {
      throw new TiBatchWriteException(
        s"invalid transaction tso with startTs=$startTs, commitTs=$commitTs"
      )
    }
    val commitPrimaryBackoff = ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF)
    ti2PCClient.commitPrimaryKey(commitPrimaryBackoff, primaryKey.bytes, commitTs)

    // executors secondary commit
    finalWriteRDD.foreachPartition {
      case iterator =>
        val tiSessionOnExecutor = new TiSession(tiConf)
        val kvClientOnExecutor = tiSessionOnExecutor.createTxnClient()
        val ti2PCClientOnExecutor = new TiBatchWrite2PCClient(kvClientOnExecutor, startTs)
        val commitSecondaryBackoff =
          ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF)

        import scala.collection.JavaConverters._
        val keys = iterator.map {
          case (key, row) => new TiBatchWrite2PCClient.ByteWrapper(key.bytes)
        }.asJava

        try {
          ti2PCClientOnExecutor.commitSecondaryKeys(commitSecondaryBackoff, keys, commitTs)
        } catch {
          case e: TiBatchWriteException =>
            // ignored
            logger.warn(s"commit secondary key error", e)
        }
    }
  }

  @throws(classOf[NoSuchTableException])
  private def getTableInfo(tableRef: TiTableReference,
                           tiContext: TiContext): (TiTableInfo, Array[DataType], TLongArrayList) = {
    val tiTableInfo =
      tiContext.tiSession.getCatalog.getTable(tableRef.databaseName, tableRef.tableName)
    if (tiTableInfo == null) {
      throw new NoSuchTableException(tableRef.databaseName, tableRef.tableName)
    }
    val tableColSize = tiTableInfo.getColumns.size()
    val dataTypes = new Array[DataType](tableColSize)
    val ids = new TLongArrayList

    for (i <- 0 until tableColSize) {
      val tiColumnInfo = tiTableInfo.getColumn(i)
      ids.add(tiColumnInfo.getId)
      dataTypes.update(i, tiColumnInfo.getType)
    }
    (tiTableInfo, dataTypes, ids)
  }

  @throws(classOf[NoSuchTableException])
  private def shuffleKeyToSameRegion(rdd: RDD[TiRow],
                                     tableRef: TiTableReference,
                                     tiTableInfo: TiTableInfo,
                                     tiContext: TiContext): RDD[(SerializableKey, TiRow)] = {
    val databaseName = tableRef.databaseName
    val tableName = tableRef.tableName
    val session = tiContext.tiSession
    val table = session.getCatalog.getTable(databaseName, tableName)
    if (table == null) {
      throw new NoSuchTableException(databaseName, tableName)
    }
    val tableId = table.getId

    val regions = getRegions(table, tiContext)
    val tiRegionPartitioner = new TiRegionPartitioner(regions)

    logger.info(
      s"find ${regions.size} regions in database: $databaseName table: $tableName tableId: $tableId"
    )

    rdd
      .map(row => (tiKVRow2Key(row, tableId, tiTableInfo), row))
      .groupByKey(tiRegionPartitioner)
      .map {
        case (key, iterable) =>
          // remove duplicate rows if key equals
          (key, iterable.head)
      }
  }

  private def getRegions(table: TiTableInfo, tiContext: TiContext): List[TiRegion] = {
    import scala.collection.JavaConversions._
    TiBatchWriteUtils.getRegionsByTable(tiContext.tiSession, table).toList
  }

  private def tiKVRow2Key(row: TiRow, tableId: Long, tiTableInfo: TiTableInfo): SerializableKey = {
    import scala.collection.JavaConverters._
    val handle = if (tiTableInfo.isPkHandle) {
      val columnList = tiTableInfo.getColumns.asScala
      val primaryColumn = columnList.find(_.isPrimaryKey).get
      row.getLong(primaryColumn.getOffset)
    } else {
      // TODO: auto generate a primary key if does not exists
      // pending: https://internal.pingcap.net/jira/browse/TISPARK-70
      row.getLong(0)
    }

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

  @throws(classOf[TiBatchWriteException])
  private def encodeTiRow(tiRow: TiRow,
                          colDataTypes: Array[DataType],
                          colIDs: TLongArrayList): Array[Byte] = {
    val colSize = tiRow.fieldCount()
    val tableColSize = colDataTypes.length

    if (colSize != tableColSize) {
      throw new TiBatchWriteException(s"col size $colSize != table column size $tableColSize")
    }

    // TODO: ddl state change
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-82
    val values = new Array[AnyRef](colSize)
    for (i <- 0 until colSize) {
      values.update(i, tiRow.get(i, colDataTypes(i)))
    }

    TableCodec.encodeRow(colDataTypes, colIDs, values, new CodecDataOutput())
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
  override def toString: String = KeyUtils.formatBytes(bytes)

  override def equals(that: Any): Boolean =
    that match {
      case that: SerializableKey => this.bytes.sameElements(that.bytes)
      case _                     => false
    }
}
