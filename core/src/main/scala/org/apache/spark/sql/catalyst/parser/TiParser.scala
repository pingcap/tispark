/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  @scala.throws[ParseException]("Text cannot be parsed to a DataType")
  def parseRawDataType(sqlText: String): DataType = ???

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
