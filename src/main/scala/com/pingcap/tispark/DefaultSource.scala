package com.pingcap.tispark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider


class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): TiDBRelation = {
    TiDBRelation(new TiOptions)(sqlContext)
  }
}
