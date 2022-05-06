package org.apache.spark.sql.catalyst.parser

import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.catalyst.catalog.TiCatalog
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, TiContext}

case class TiParser(
    getOrCreateTiContext: SparkSession => TiContext,
    sparkSession: SparkSession,
    delegate: ParserInterface)
    extends ParserInterface {
  private lazy val tiContext = getOrCreateTiContext(sparkSession)

  override def parsePlan(sqlText: String): LogicalPlan = {
    updateMetaCatalog
    delegate.parsePlan(sqlText)
  }

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  override def parseRawDataType(sqlText: String): DataType = delegate.parseRawDataType(sqlText)

  def getOrElseInitTiCatalog: TiCatalog = {
    val catalogManager = sparkSession.sessionState.catalogManager
    catalogManager.catalog("tidb_catalog").asInstanceOf[TiCatalog]
  }

  def updateMetaCatalog: Unit = {
    val meta = getOrElseInitTiCatalog.meta.get
    val timeStamp = TiUtil.getTiDBSnapshot(sparkSession)
    val catalog = if (timeStamp.isEmpty) {
      tiContext.tiSession.getCatalog
    } else {
      tiContext.tiSession.getOrCreateSnapShotCatalog(timeStamp.get)
    }
    meta.reloadCatalog(catalog)
  }
}
