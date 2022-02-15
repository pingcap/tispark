package com.pingcap.tispark

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, LogicalWriteInfo}
import org.apache.spark.sql.types.StructType

class TiDBDataWriterFactory(schema: StructType, tiDBOptions: TiDBOptions, ticonf: TiConfiguration)
    extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    TiDBDataWrite(partitionId, taskId, schema, tiDBOptions, ticonf)
}
