package com.pingcap.tispark

import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  LogicalWriteInfo,
  PhysicalWriteInfo,
  WriterCommitMessage
}

class TiDBBatchWrite(logicalInfo: LogicalWriteInfo, tiDBOptions: TiDBOptions)(
    @transient val tiContext: TiContext)
    extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new TiDBDataWriterFactory(logicalInfo.schema(), tiDBOptions, tiContext.tiConf)

  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}
