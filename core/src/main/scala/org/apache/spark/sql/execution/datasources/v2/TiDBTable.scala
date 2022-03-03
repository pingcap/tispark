package org.apache.spark.sql.execution.datasources.v2

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.key.Handle
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo, TiTimestamp}
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.write.TiDBOptions
import com.pingcap.tispark.{MetaManager, TiTableReference}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
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
    meta: MetaManager,
    var ts: TiTimestamp = null,
    options: Option[TiDBOptions] = None)(@transient val sqlContext: SQLContext)
    extends SupportsRead {

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

  lazy val table: TiTableInfo = meta
    .getTable(tableRef.databaseName, tableRef.tableName)
    .getOrElse(throw new NoSuchTableException(tableRef.databaseName, tableRef.tableName))
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

//  override lazy val partitioning: Array[Transform] = {
//    val partitions = new mutable.ArrayBuffer[Transform]()
//
//    v1Table.partitionColumnNames.foreach { col =>
//      partitions += LogicalExpressions.identity(FieldReference(col))
//    }
//
//    v1Table.bucketSpec.foreach { spec =>
//      partitions += LogicalExpressions
//        .bucket(spec.numBuckets, spec.bucketColumnNames.map(FieldReference(_)).toArray)
//    }
//
//    partitions.toArray
//  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    () => () => schema

  override def name(): String = tableRef.quoted

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = new util.HashSet[TableCapability]
    capabilities.add(TableCapability.BATCH_READ)
    capabilities
  }

  override def toString: String = s"TiDBTable($name)"

  def getTiFlashReplicaProgress: Double = {
    import scala.collection.JavaConversions._
    val progress = table.getPartitionInfo.getDefs
      .map(partitonDef => session.getPDClient.getTiFlashReplicaProgress(partitonDef.getId))
      .sum
    progress / table.getPartitionInfo.getDefs.size()
  }

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

}
