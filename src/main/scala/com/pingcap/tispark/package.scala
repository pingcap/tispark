package com.pingcap.tispark

import org.apache.spark.sql.{DataFrame, SQLContext}


package object tispark {
  implicit class TiContext(sqlContext: SQLContext) extends Serializable{
    def tidbTable(
                 tiAddress: List[String],
                 dbName: String,
                 tableName: String
                 ): DataFrame = {
      val tiRelation = TiDBRelation(new TiOptions)(sqlContext)
      sqlContext.baseRelationToDataFrame(tiRelation)
    }
  }
}
