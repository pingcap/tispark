package com.pingcap.tispark

import com.pingcap.tikv.{TiCluster, TiConfiguration}
import org.apache.spark.rdd.RDD
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

  override def logicalPlanToRDD(plan: LogicalPlan): RDD[Row] = {
    new TiRDD(TiUtils.coprocessorReqToBytes(plan).toProtoByteString(),
              sqlContext.sparkContext, options)
  }
}
