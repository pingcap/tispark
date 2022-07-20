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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.v2

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.Handle
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo, TiTimestamp}
import com.pingcap.tispark.TiTableReference
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.v2.TiDBTable.{getDagRequestToRegionTaskExec, getLogicalPlanToRDD}
import com.pingcap.tispark.v2.sink.TiDBWriteBuilder
import com.pingcap.tispark.write.{TiDBDelete, TiDBOptions}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.connector.catalog.{
  SupportsDelete,
  SupportsRead,
  SupportsWrite,
  TableCapability
}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.{ColumnarCoprocessorRDD, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRowRDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{SQLContext, execution}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.sql.{Date, SQLException, Timestamp}
import java.time.{Instant, LocalDate}
import java.util
import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class TiDBTable(
    session: TiSession,
    tableRef: TiTableReference,
    table: TiTableInfo,
    var ts: TiTimestamp = null,
    options: Option[Map[String, String]] = None)(@transient val sqlContext: SQLContext)
    extends SupportsRead
    with SupportsWrite
    with SupportsDelete {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

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
      options.get.asJava
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
    TiDBWriteBuilder(info, tiDBOptions, sqlContext)
  }

  override def deleteWhere(filters: Array[Filter]): Unit = {
    //Check filters
    require(filters.length > 0, "Delete without WHERE clause is not supported")
    require(
      !(filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue]),
      "Delete with alwaysTrue WHERE clause is not supported")

    // Parse filters to WHERE clause
    val filterWhereClause: String =
      filters
        .flatMap(TiDBTable.compileFilter)
        .map(p => s"($p)")
        .mkString(" AND ")
    if (StringUtils.isEmpty(filterWhereClause)) {
      throw new SQLException("D fail: can't parse WHERE Clause")
    }

    // TODO It's better to use the start_ts of read. We can't get it now.
    val startTs = session.getTimestamp.getVersion
    logger.info(s"startTS: $startTs")

    // Query data from TiKV (ByPass TiDB)
    val df = sqlContext.sparkSession.read
      .format("tidb")
      .option(TiDBOptions.TIDB_DATABASE, tableRef.databaseName)
      .option(TiDBOptions.TIDB_TABLE, tableRef.tableName)
      .option(TiDBOptions.TIDB_ROWID, "true")
      .load()
      .filter(filterWhereClause)

    // get TiDBOptions
    val tidbOptions = new TiDBOptions(sqlContext.sparkSession.conf.getAll)

    // Execute delete
    val tiDBDelete =
      TiDBDelete(df, databaseName, tableName, startTs, Some(tidbOptions))
    try {
      tiDBDelete.delete()
    } finally {
      tiDBDelete.unpersistAll()
    }
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

  /**
   * convert Filter to WHERE clause
   * update it when spark has new filters in [[org.apache.spark.sql.sources.Filter]]
   */
  def compileFilter(f: Filter): Option[String] = {
    // in case colName is a reserved keyword
    def quote(colName: String): String = s"`$colName`"

    // This behavior is different from TiDB. TiDB use '' to escaple ' , Spark use \\' to escape '
    def escapeSql(value: String): String =
      if (value == null) null else StringUtils.replace(value, "'", "\\'")

    // compile value to correct data type
    def compileValue(value: Any): Any =
      value match {
        case stringValue: String => s"'${escapeSql(stringValue)}'"
        case timestampValue: Timestamp => "'" + timestampValue + "'"
        case timestampValue: Instant =>
          val timestampFormatter = TimestampFormatter.getFractionFormatter(
            DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
          s"'${timestampFormatter.format(timestampValue)}'"
        case dateValue: Date => "'" + dateValue + "'"
        case dateValue: LocalDate => "'" + dateValue + "'"
        case arrayByte: Array[Byte] => "'" + new String(arrayByte, StandardCharsets.UTF_8) + "'"
        case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
        case _ => value
      }

    Option(f match {
      case AlwaysFalse => "false"
      case EqualTo(attr, value) => s"${quote(attr)} = ${compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        val col = quote(attr)
        s"(NOT ($col != ${compileValue(value)} OR $col IS NULL OR " +
          s"${compileValue(value)} IS NULL) OR " +
          s"($col IS NULL AND ${compileValue(value)} IS NULL))"
      case LessThan(attr, value) => s"${quote(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${quote(attr)} > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${quote(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${quote(attr)} >= ${compileValue(value)}"
      case IsNull(attr) => s"${quote(attr)} IS NULL"
      case IsNotNull(attr) => s"${quote(attr)} IS NOT NULL"
      case StringStartsWith(attr, value) => s"${quote(attr)} LIKE '${value}%'"
      case StringEndsWith(attr, value) => s"${quote(attr)} LIKE '%${value}'"
      case StringContains(attr, value) => s"${quote(attr)} LIKE '%${value}%'"
      case In(attr, value) if value.isEmpty =>
        s"CASE WHEN ${quote(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr, value) => s"${quote(attr)} IN (${compileValue(value)})"
      case Not(f) => compileFilter(f).map(p => s"(NOT ($p))").orNull
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter)
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter)
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

}
