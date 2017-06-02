package com.pingcap.tispark

import com.pingcap.tikv.catalog.Catalog
import com.pingcap.tikv.meta.{TiDBInfo, TiRange, TiTableInfo}
import com.pingcap.tikv.{SelectBuilder, TiCluster, TiConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{BaseRelation, CatalystSource}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConverters._

case class TiDBRelation(options: TiOptions)(@transient val sqlContext: SQLContext)
  extends BaseRelation with CatalystSource {

  val conf: TiConfiguration = TiConfiguration.createDefault(options.addresses.asJava)
  val cluster: TiCluster = TiCluster.getCluster(conf)
  val catalog: Catalog = cluster.getCatalog
  val database: TiDBInfo = catalog.getDatabase(options.databaseName)
  val table: TiTableInfo = catalog.getTable(database, options.tableName)

  override def schema: StructType = {
    val fields = new Array[StructField](table.getColumns.size())
    for (i <- 0 until table.getColumns.size()) {
      val col = table.getColumns.get(i)
      val metadata = new MetadataBuilder()
            .putString("name", col.getName)
            .build()
      fields(i) = StructField(col.getName, TiUtils.toSparkDataType(col.getType), nullable = true, metadata)
    }
    new StructType(fields)
  }

  override def tableInfo :TiTableInfo = {
    table
  }

  override def logicalPlanToRDD(plan: LogicalPlan): RDD[Row] = {
    new TiRDD(TiUtils.coprocessorReqToBytes(plan, builder = SelectBuilder.newBuilder(cluster.createSnapshot(), table)).addRange(TiRange.create[java.lang.Long](0L, Long.MaxValue)).build().toByteString,
              List(TiRange.create[java.lang.Long](0L, Long.MaxValue)),
              sqlContext.sparkContext, options)
  }
}
