package com.pingcap.tispark

import org.apache.spark.sql.{DataFrame, SQLContext}


package object tispark {
  implicit class TiContext(sqlContext: SQLContext) extends Serializable{
    def tidbTable(
                 tiAddresses: List[String],
                 dbName: String,
                 tableName: String
                 ): DataFrame = {
      val tiRelation = TiDBRelation(new TiOptions(tiAddresses, dbName, tableName))(sqlContext)
      sqlContext.baseRelationToDataFrame(tiRelation)
    }
  }
}
