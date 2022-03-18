/*
 * Copyright 2021 PingCAP, Inc.
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

package com.pingcap.tispark.v2

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.Handle
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo, TiTimestamp}
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.v2.TiDBTable.{getDagRequestToRegionTaskExec, getLogicalPlanToRDD}
import com.pingcap.tispark.v2.sink.TiDBWriterBuilder
import com.pingcap.tispark.write.TiDBOptions
import com.pingcap.tispark.TiTableReference
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.{ColumnarCoprocessorRDD, SparkPlan}
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRowRDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{SQLContext, execution}

import java.util
import java.util.Collections
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

case class TiDBTable(
    session: TiSession,
    tableRef: TiTableReference,
    table: TiTableInfo,
    var ts: TiTimestamp = null,
    options: Option[TiDBOptions] = None)(@transient val sqlContext: SQLContext)
    extends SupportsRead
    with SupportsWrite {

  implicit class IdentifierHelper(identifier: TiTableReference) {
    def quoted: String = {
      Seq(identifier.databaseName, identifier.tableName).map(quote).mkString(".")
    }
    private def quote(part: String): String = {
      if (part.contains(".") || part.contains("`")) {
        s"`${part.replace("`", "``")}`"
      } else {
        part
      }
    }
  }

  override lazy val schema: StructType = TiUtil.getSchemaFromTable(table)

  override lazy val properties: util.Map[String, String] = {
    if (options.isEmpty) {
      Collections.emptyMap()
    } else {
      options.get.parameters.toMap.asJava
    }
  }

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

  def databaseName: String = tableRef.databaseName
  def tableName: String = tableRef.tableName

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    () => () => schema

  override def name(): String = tableRef.quoted

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = new util.HashSet[TableCapability]
    capabilities.add(TableCapability.BATCH_READ)
    capabilities.add(TableCapability.V1_BATCH_WRITE)
    capabilities
  }

  override def toString: String = s"TiDBTable($name)"

  def dagRequestToRegionTaskExec(dagRequest: TiDAGRequest, output: Seq[Attribute]): SparkPlan = {
    getDagRequestToRegionTaskExec(dagRequest, output, session, sqlContext, tableRef)
  }

  def logicalPlanToRDD(dagRequest: TiDAGRequest, output: Seq[Attribute]): List[TiRowRDD] = {
    getLogicalPlanToRDD(dagRequest, output, session, sqlContext, tableRef)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    var scalaMap = info.options().asScala.toMap
    // TODO https://github.com/pingcap/tispark/issues/2269 we need to move TiDB dependencies which will block insert SQL.
    // if we don't support it before release, insert SQL should throw exception in catalyst
    if (scalaMap.isEmpty) {
      throw new TiBatchWriteException("tidbOption is neccessary.")
    }
    // Support df.writeto: need add db and table for write
    if (!scalaMap.contains("database")) {
      scalaMap += ("database" -> databaseName)
    }
    if (!scalaMap.contains("table")) {
      scalaMap += ("table" -> tableName)
    }
    // Get TiDBOptions
    val tiDBOptions = new TiDBOptions(scalaMap)
    TiDBWriterBuilder(info, tiDBOptions, sqlContext)
  }
}

object TiDBTable {

  private def getDagRequestToRegionTaskExec(
      dagRequest: TiDAGRequest,
      output: Seq[Attribute],
      session: TiSession,
      sqlContext: SQLContext,
      tableRef: TiTableReference): SparkPlan = {
    import scala.collection.JavaConverters._
    val ids = dagRequest.getPrunedPhysicalIds.asScala
    val tiHandleRDDs = new ListBuffer[TiHandleRDD]()
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

  private def getLogicalPlanToRDD(
      dagRequest: TiDAGRequest,
      output: Seq[Attribute],
      session: TiSession,
      sqlContext: SQLContext,
      tableRef: TiTableReference): List[TiRowRDD] = {
    import scala.collection.JavaConverters._
    val ids = dagRequest.getPrunedPhysicalIds.asScala
    val tiRDDs = new ListBuffer[TiRowRDD]
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
}
