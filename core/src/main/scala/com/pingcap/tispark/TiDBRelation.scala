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

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.{TiBatchWriteException, TiClientInternalException}
import com.pingcap.tikv.key.Handle
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo, TiTimestamp}
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.write.{TiDBOptions, TiDBWriter}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRowRDD}
import org.apache.spark.sql.types.{ArrayType, LongType, Metadata, ObjectType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, execution}

import scala.collection.mutable.ListBuffer

case class TiDBRelation(
    session: TiSession,
    tableRef: TiTableReference,
    meta: MetaManager,
    var ts: TiTimestamp = null,
    options: Option[TiDBOptions] = None)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with InsertableRelation {
  lazy val table: TiTableInfo = getTableOrThrow(tableRef.databaseName, tableRef.tableName)

  override lazy val schema: StructType = TiUtil.getSchemaFromTable(table)
  lazy val isTiFlashReplicaAvailable: Boolean = {
    // Note:
    // - INFORMATION_SCHEMA.TIFLASH_REPLICA is not present in TiKV or PD,
    // it is calculated in TiDB and stored in memory.
    // - In order to get those helpful information we have to read them from
    // either TiKV or PD and keep them in memory as well.
    //
    // select * from INFORMATION_SCHEMA.TIFLASH_REPLICA where table_id = $id
    // TABLE_SCHEMA, TABLE_NAME, TABLE_ID, REPLICA_COUNT, LOCATION_LABELS, AVAILABLE, PROGRESS
    table.getTiflashReplicaInfo != null && table.getTiflashReplicaInfo.isAvailable
  }

  def getTiFlashReplicaProgress: Double = {
    import scala.collection.JavaConversions._
    val progress = table.getPartitionInfo.getDefs
      .map(partitonDef => session.getPDClient.getTiFlashReplicaProgress(partitonDef.getId))
      .sum
    progress / table.getPartitionInfo.getDefs.size()
  }

  override def sizeInBytes: Long = tableRef.sizeInBytes

  def logicalPlanToRDD(dagRequest: TiDAGRequest, output: Seq[Attribute]): List[TiRowRDD] = {
    import scala.collection.JavaConverters._
    val ids = dagRequest.getPrunedPhysicalIds.asScala
    var tiRDDs = new ListBuffer[TiRowRDD]
    val tiConf = session.getConf
    tiConf.setPartitionPerSplit(TiUtil.getPartitionPerSplit(sqlContext))
    ids.foreach(id => {
      tiRDDs += new TiRowRDD(
        dagRequest.copyReqWithPhysicalId(id),
        id,
        TiUtil.getChunkBatchSize(sqlContext),
        tiConf,
        output,
        tableRef,
        session,
        sqlContext.sparkSession)
    })
    tiRDDs.toList
  }

  def dagRequestToRegionTaskExec(dagRequest: TiDAGRequest, output: Seq[Attribute]): SparkPlan = {
    import scala.collection.JavaConverters._
    val ids = dagRequest.getPrunedPhysicalIds.asScala
    var tiHandleRDDs = new ListBuffer[TiHandleRDD]()
    lazy val attributeRef = Seq(
      AttributeReference("RegionId", LongType, nullable = false, Metadata.empty)(),
      AttributeReference(
        "Handles",
        ArrayType(ObjectType(classOf[Handle]), containsNull = false),
        nullable = false,
        Metadata.empty)())

    val tiConf = session.getConf
    tiConf.setPartitionPerSplit(TiUtil.getPartitionPerSplit(sqlContext))
    ids.foreach(id => {
      tiHandleRDDs +=
        new TiHandleRDD(
          dagRequest,
          id,
          attributeRef,
          tiConf,
          tableRef,
          session,
          sqlContext.sparkSession)
    })

    // TODO: we may optimize by partitioning the result by region.
    // https://github.com/pingcap/tispark/issues/1200
    val handlePlan = ColumnarCoprocessorRDD(attributeRef, tiHandleRDDs.toList, fetchHandle = true)
    execution.ColumnarRegionTaskExec(
      handlePlan,
      output,
      TiUtil.getChunkBatchSize(sqlContext),
      dagRequest,
      session.getConf,
      session.getTimestamp,
      session,
      sqlContext.sparkSession)
  }

  override def equals(obj: Any): Boolean =
    obj match {
      case other: TiDBRelation =>
        this.table.equals(other.table)
      case _ =>
        false
    }

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    // default forbid sql interface
    // cause tispark provide `replace` instead of `insert` semantic
    if (session.getConf.isWriteAllowSparkSQL) {
      val saveMode = if (overwrite) {
        SaveMode.Overwrite
      } else {
        SaveMode.Append
      }
      TiDBWriter.write(data, sqlContext, saveMode, options.get)
    } else {
      throw new TiBatchWriteException(
        "SparkSQL entry for tispark write is disabled. Set spark.tispark.write.allow_spark_sql to enable.")
    }

  override def toString: String = {
    s"TiDBRelation($tableRef, $ts)"
  }

  private def getTableOrThrow(database: String, table: String): TiTableInfo =
    meta.getTable(database, table).getOrElse {
      val db = meta.getDatabase(database)
      if (db.isEmpty) {
        throw new TiClientInternalException(
          "Database not exist " + database + " valid databases are: " + meta.getDatabases
            .map(_.getName)
            .mkString("[", ",", "]"))
      } else {
        throw new TiClientInternalException(
          "Table not exist " + tableRef + " valid tables are: " + meta
            .getTables(db.get)
            .map(_.getName)
            .mkString("[", ",", "]"))
      }
    }
}
