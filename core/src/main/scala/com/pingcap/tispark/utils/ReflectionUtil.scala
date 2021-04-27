/*
 *
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.utils

import com.pingcap.tispark.TiSparkInfo
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.catalyst.expressions.BasicExpression.TiExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression, SortOrder}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.slf4j.LoggerFactory

import java.io.File
import java.net.{URL, URLClassLoader}

/**
 * ReflectionUtil is designed to reflect methods which differ across
 * different Spark versions. Compatibility issues should be solved by
 * reflections in future.
 */
object ReflectionUtil {
  lazy val classLoader: URLClassLoader = {
    val tisparkClassUrl = this.getClass.getProtectionDomain.getCodeSource.getLocation
    val tisparkClassPath = new File(tisparkClassUrl.getFile)
    logger.info(s"tispark class url: ${tisparkClassUrl.toString}")

    val sparkWrapperClassURL: URL = if (tisparkClassPath.isDirectory) {
      val classDir = new File(
        s"${tisparkClassPath.getAbsolutePath}/../../../spark-wrapper/spark-${TiSparkInfo.SPARK_MAJOR_VERSION}/target/classes/")
      if (!classDir.exists()) {
        throw new Exception(
          "cannot find spark wrapper classes! please compile the spark-wrapper project first!")
      }
      classDir.toURI.toURL
    } else {
      new URL(
        s"jar:$tisparkClassUrl!/resources/spark-wrapper-spark-${TiSparkInfo.SPARK_MAJOR_VERSION
          .replace('.', '_')}/")
    }
    logger.info(s"spark wrapper class url: ${sparkWrapperClassURL.toString}")

    new URLClassLoader(Array(sparkWrapperClassURL), this.getClass.getClassLoader)
  }
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val SPARK_WRAPPER_CLASS = "com.pingcap.tispark.SparkWrapper"
  private val TI_BASIC_EXPRESSION_CLASS =
    "org.apache.spark.sql.catalyst.expressions.TiBasicExpression"
  private val TI_RESOLUTION_RULE_CLASS =
    "org.apache.spark.sql.extensions.TiResolutionRule"
  private val TI_RESOLUTION_RULE_V2_CLASS =
    "org.apache.spark.sql.extensions.TiResolutionRuleV2"
  private val TI_PARSER_CLASS =
    "org.apache.spark.sql.extensions.TiParser"
  private val TI_DDL_RULE_CLASS =
    "org.apache.spark.sql.extensions.TiDDLRule"

  def newAlias(child: Expression, name: String): Alias = {
    classLoader
      .loadClass(SPARK_WRAPPER_CLASS)
      .getDeclaredMethod("newAlias", classOf[Expression], classOf[String])
      .invoke(null, child, name)
      .asInstanceOf[Alias]
  }

  def newAlias(child: Expression, name: String, exprId: ExprId): Alias = {
    classLoader
      .loadClass(SPARK_WRAPPER_CLASS)
      .getDeclaredMethod("newAlias", classOf[Expression], classOf[String], classOf[ExprId])
      .invoke(null, child, name, exprId)
      .asInstanceOf[Alias]
  }

  def copySortOrder(sortOrder: SortOrder, child: Expression): SortOrder = {
    classLoader
      .loadClass(SPARK_WRAPPER_CLASS)
      .getDeclaredMethod("copySortOrder", classOf[SortOrder], classOf[Expression])
      .invoke(null, sortOrder, child)
      .asInstanceOf[SortOrder]
  }

  def trimNonTopLevelAliases(e: Expression): Expression = {
    classLoader
      .loadClass(SPARK_WRAPPER_CLASS)
      .getDeclaredMethod("trimNonTopLevelAliases", classOf[Expression])
      .invoke(null, e)
      .asInstanceOf[Expression]
  }

  def callTiBasicExpressionConvertToTiExpr(expr: Expression): Option[TiExpression] = {
    classLoader
      .loadClass(TI_BASIC_EXPRESSION_CLASS)
      .getDeclaredMethod("convertToTiExpr", classOf[Expression])
      .invoke(null, expr)
      .asInstanceOf[Option[TiExpression]]
  }

  def newTiResolutionRule(
      getOrCreateTiContext: SparkSession => TiContext,
      sparkSession: SparkSession): Rule[LogicalPlan] = {
    classLoader
      .loadClass(TI_RESOLUTION_RULE_CLASS)
      .getDeclaredConstructor(classOf[SparkSession => TiContext], classOf[SparkSession])
      .newInstance(getOrCreateTiContext, sparkSession)
      .asInstanceOf[Rule[LogicalPlan]]
  }

  def newTiResolutionRuleV2(
      getOrCreateTiContext: SparkSession => TiContext,
      sparkSession: SparkSession): Rule[LogicalPlan] = {
    classLoader
      .loadClass(TI_RESOLUTION_RULE_V2_CLASS)
      .getDeclaredConstructor(classOf[SparkSession => TiContext], classOf[SparkSession])
      .newInstance(getOrCreateTiContext, sparkSession)
      .asInstanceOf[Rule[LogicalPlan]]
  }

  def newTiParser(
      getOrCreateTiContext: SparkSession => TiContext,
      sparkSession: SparkSession,
      parserInterface: ParserInterface): ParserInterface = {
    classLoader
      .loadClass(TI_PARSER_CLASS)
      .getDeclaredConstructor(
        classOf[SparkSession => TiContext],
        classOf[SparkSession],
        classOf[ParserInterface])
      .newInstance(getOrCreateTiContext, sparkSession, parserInterface)
      .asInstanceOf[ParserInterface]
  }

  def newTiDDLRule(
      getOrCreateTiContext: SparkSession => TiContext,
      sparkSession: SparkSession): Rule[LogicalPlan] = {
    classLoader
      .loadClass(TI_DDL_RULE_CLASS)
      .getDeclaredConstructor(classOf[SparkSession => TiContext], classOf[SparkSession])
      .newInstance(getOrCreateTiContext, sparkSession)
      .asInstanceOf[Rule[LogicalPlan]]
  }
}
