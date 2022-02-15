package com.pingcap.tispark

import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, WriteBuilder}

class TiDBWriterBuilder(info: LogicalWriteInfo, tiDBOptions: TiDBOptions)(
    @transient val tiContext: TiContext)
    extends WriteBuilder {

  override def buildForBatch(): BatchWrite = new TiDBBatchWrite(info, tiDBOptions)(tiContext)
}
