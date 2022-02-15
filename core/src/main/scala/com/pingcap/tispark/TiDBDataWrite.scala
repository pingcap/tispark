package com.pingcap.tispark

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tispark.write.{TiDBOptions, TiDBWriter}
import org.apache.spark.sql.{Row, SaveMode, SparkSession, TiContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

case class TiDBDataWrite(
    partitionId: Int,
    taskId: Long,
    schema: StructType,
    tiDBOptions: TiDBOptions,
    ticonf: TiConfiguration)
    extends DataWriter[InternalRow] {

  val dataBuf = ListBuffer[Row]()

  override def write(record: InternalRow): Unit = {
    //val encoder = RowEncoder(schema)
    val row = Row.fromSeq(record.toSeq(schema))
    dataBuf.append(row)

  }

  override def commit(): WriterCommitMessage = {
    WriteSucceeded
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
object WriteSucceeded extends WriterCommitMessage
