package com.pingcap.tispark

import com.google.proto4pingcap.ByteString
import com.pingcap.tikv.{TiCluster, TiConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{BaseRelation, CatalystSource}
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConverters._


case class TiDBRelation(options: TiOptions)(@transient val sqlContext: SQLContext)
  extends BaseRelation with CatalystSource {

  val conf = TiConfiguration.createDefault(options.addresses.asJava)
  val cluster = TiCluster.getCluster(conf)

  override def schema: StructType = {
    val catalog = cluster.getCatalog()
    val database = catalog.getDatabase(options.databaseName)
    val table = catalog.getTable(database, options.tableName)
    val fields = new Array[StructField](table.getColumns.size())
    for (i <- 0 until table.getColumns.size()) {
      val col = table.getColumns.get(i)
      val metadata = new MetadataBuilder()
            .putString("name", col.getName)
            .build()
      fields(i) = StructField(col.getName, LongType, false, metadata)
    }
    new StructType(fields)
  }

  //def buildScan(cop: CoprocessorReq): RDD[Row] = {
  //  new TiRDD(coprocessorReqToBytes(cop), sqlContext.sparkContext, options)
  //}
  /**
    * {@inheritDoc}
    */
  override def isMultiplePartitionExecution(relations: Seq[CatalystSource]): Boolean = {
    true
  }

  /**
    * {@inheritDoc}
    */
  override def supportsLogicalPlan(plan: LogicalPlan): Boolean = {
    true
  }

  /**
    * {@inheritDoc}
    */
  override def supportsExpression(expr: Expression): Boolean = {
    true
  }

  /**
    * {@inheritDoc}
    */
  override def logicalPlanToRDD(plan: LogicalPlan): RDD[Row] = {
    new TiRDD(coprocessorReqToBytes(plan), sqlContext.sparkContext, options)
  }

  private def coprocessorReqToBytes(plan: LogicalPlan): ByteString = {
    ByteString.copyFromUtf8(plan.toJSON)
  }
}
