package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.{SparkSession, TiContext}

case class TiParserFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends ((SparkSession, ParserInterface) => ParserInterface) {
  override def apply(
      sparkSession: SparkSession,
      parserInterface: ParserInterface): ParserInterface = {
    TiParser(getOrCreateTiContext, sparkSession, parserInterface)
  }
}
