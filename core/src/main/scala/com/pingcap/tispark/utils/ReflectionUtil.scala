/*
 * Copyright 2019 PingCAP, Inc.
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

import java.io.File
import java.lang.reflect.Method
import java.net.{URL, URLClassLoader}

import com.pingcap.tispark.TiSparkInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{
  CatalogTable,
  ExternalCatalog,
  SessionCatalog,
  TiSessionCatalog
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  Expression,
  NamedExpression,
  UnsafeRow
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.types.{DataType, Metadata}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

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
        s"jar:$tisparkClassUrl!/resources/spark-wrapper-spark-${TiSparkInfo.SPARK_MAJOR_VERSION}/")
    }
    logger.info(s"spark wrapper class url: ${sparkWrapperClassURL.toString}")

    new URLClassLoader(Array(sparkWrapperClassURL), this.getClass.getClassLoader)
  }
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val SPARK_WRAPPER_CLASS = "com.pingcap.tispark.SparkWrapper"
  private val TI_AGGREGATION_IMPL_CLASS = "org.apache.spark.sql.TiAggregationImpl"
  private val TI_DIRECT_EXTERNAL_CATALOG_CLASS =
    "org.apache.spark.sql.catalyst.catalog.TiDirectExternalCatalog"
  private val TI_COMPOSITE_SESSION_CATALOG_CLASS =
    "org.apache.spark.sql.catalyst.catalog.TiCompositeSessionCatalog"

  def newTiDirectExternalCatalog(tiContext: TiContext): ExternalCatalog = {
    classLoader
      .loadClass(TI_DIRECT_EXTERNAL_CATALOG_CLASS)
      .getDeclaredConstructor(classOf[TiContext])
      .newInstance(tiContext)
      .asInstanceOf[ExternalCatalog]
  }

  def callTiDirectExternalCatalogDatabaseExists(obj: Object, db: String): Boolean = {
    classLoader
      .loadClass(TI_DIRECT_EXTERNAL_CATALOG_CLASS)
      .getDeclaredMethod("databaseExists", classOf[String])
      .invoke(obj, db)
      .asInstanceOf[Boolean]
  }

  def callTiDirectExternalCatalogTableExists(obj: Object, db: String, table: String): Boolean = {
    classLoader
      .loadClass(TI_DIRECT_EXTERNAL_CATALOG_CLASS)
      .getDeclaredMethod("tableExists", classOf[String], classOf[String])
      .invoke(obj, db, table)
      .asInstanceOf[Boolean]
  }

  def newTiCompositeSessionCatalog(tiContext: TiContext): TiSessionCatalog = {
    classLoader
      .loadClass(TI_COMPOSITE_SESSION_CATALOG_CLASS)
      .getDeclaredConstructor(classOf[TiContext])
      .newInstance(tiContext)
      .asInstanceOf[TiSessionCatalog]
  }

  def callTiAggregationImplUnapply(plan: LogicalPlan): Option[
    (Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)] = {
    classLoader
      .loadClass(TI_AGGREGATION_IMPL_CLASS)
      .getDeclaredMethod("unapply", classOf[LogicalPlan])
      .invoke(null, plan)
      .asInstanceOf[Option[
        (Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)]]
  }

  def newSubqueryAlias(identifier: String, child: LogicalPlan): SubqueryAlias = {
    classLoader
      .loadClass(SPARK_WRAPPER_CLASS)
      .getDeclaredMethod("newSubqueryAlias", classOf[String], classOf[LogicalPlan])
      .invoke(null, identifier, child)
      .asInstanceOf[SubqueryAlias]
  }

  def newAlias(child: Expression, name: String): Alias = {
    classLoader
      .loadClass(SPARK_WRAPPER_CLASS)
      .getDeclaredMethod("newAlias", classOf[Expression], classOf[String])
      .invoke(null, child, name)
      .asInstanceOf[Alias]
  }

  def newAttributeReference(
      name: String,
      dataType: DataType,
      nullable: java.lang.Boolean = false,
      metadata: Metadata = Metadata.empty): AttributeReference = {
    classLoader
      .loadClass(SPARK_WRAPPER_CLASS)
      .getDeclaredMethod(
        "newAttributeReference",
        classOf[String],
        classOf[DataType],
        classOf[Boolean],
        classOf[Metadata])
      .invoke(null, name, dataType, nullable, metadata)
      .asInstanceOf[AttributeReference]
  }

  def callSessionCatalogCreateTable(
      obj: SessionCatalog,
      tableDefinition: CatalogTable,
      ignoreIfExists: java.lang.Boolean): Unit = {
    classLoader
      .loadClass(SPARK_WRAPPER_CLASS)
      .getDeclaredMethod(
        "callSessionCatalogCreateTable",
        classOf[SessionCatalog],
        classOf[CatalogTable],
        classOf[Boolean])
      .invoke(null, obj, tableDefinition, ignoreIfExists)
  }

  // Spark-2.3.0 & Spark-2.3.1
  private def reflectMapPartitionsWithIndexInternalV1(
      rdd: RDD[InternalRow],
      internalRowToUnsafeRowWithIndex: (Int, Iterator[InternalRow]) => Iterator[InternalRow])
      : (String, Method) = {
    (
      "v1",
      classOf[RDD[InternalRow]].getDeclaredMethod(
        "mapPartitionsWithIndexInternal",
        classOf[(Int, Iterator[InternalRow]) => Iterator[InternalRow]],
        classOf[Boolean],
        classOf[ClassTag[UnsafeRow]]))
  }

  // >= Spark-2.3.2
  private def reflectMapPartitionsWithIndexInternalV2(
      rdd: RDD[InternalRow],
      internalRowToUnsafeRowWithIndex: (Int, Iterator[InternalRow]) => Iterator[InternalRow])
      : (String, Method) = {
    (
      "v2",
      classOf[RDD[InternalRow]].getDeclaredMethod(
        "mapPartitionsWithIndexInternal",
        classOf[(Int, Iterator[InternalRow]) => Iterator[InternalRow]],
        classOf[Boolean],
        classOf[Boolean],
        classOf[ClassTag[UnsafeRow]]))
  }

  private def invokeMapPartitionsWithIndexInternal(
      version: String,
      method: Method,
      rdd: RDD[InternalRow],
      internalRowToUnsafeRowWithIndex: (Int, Iterator[InternalRow]) => Iterator[InternalRow])
      : RDD[InternalRow] = {
    version match {
      case "v1" =>
        // Spark-2.3.0 & Spark-2.3.1
        method
          .invoke(
            rdd,
            internalRowToUnsafeRowWithIndex,
            Boolean.box(false),
            ClassTag.apply(classOf[UnsafeRow]))
          .asInstanceOf[RDD[InternalRow]]

      case _ =>
        // >= Spark-2.3.2
        method
          .invoke(
            rdd,
            internalRowToUnsafeRowWithIndex,
            Boolean.box(false),
            Boolean.box(false),
            ClassTag.apply(classOf[UnsafeRow]))
          .asInstanceOf[RDD[InternalRow]]
    }
  }

  // In Spark 2.3.0 and 2.3.1 the method declaration is:
  // private[spark] def mapPartitionsWithIndexInternal[U: ClassTag](
  //      f: (Int, Iterator[T]) => Iterator[U],
  //      preservesPartitioning: Boolean = false): RDD[U]
  //
  // In other Spark 2.3.x versions, the method declaration is:
  // private[spark] def mapPartitionsWithIndexInternal[U: ClassTag](
  //      f: (Int, Iterator[T]) => Iterator[U],
  //      preservesPartitioning: Boolean = false,
  //      isOrderSensitive: Boolean = false): RDD[U]
  //
  // Hereby we use reflection to support different Spark versions.
  case class ReflectionMapPartitionWithIndexInternal(
      rdd: RDD[InternalRow],
      internalRowToUnsafeRowWithIndex: (Int, Iterator[InternalRow]) => Iterator[InternalRow]) {
    private val scalaReflectionException = ScalaReflectionException(
      s"Cannot find reflection of Method mapPartitionsWithIndexInternal, current Spark version is ${TiSparkInfo.SPARK_VERSION}")
    // Spark HDP Release may not compatible with official Release
    // see https://github.com/pingcap/tispark/issues/1006
    def invoke(): RDD[InternalRow] = {
      val (version, method) = TiSparkInfo.SPARK_VERSION match {
        case "2.3.0" | "2.3.1" =>
          try {
            reflectMapPartitionsWithIndexInternalV1(rdd, internalRowToUnsafeRowWithIndex)
          } catch {
            case _: Throwable =>
              try {
                reflectMapPartitionsWithIndexInternalV2(rdd, internalRowToUnsafeRowWithIndex)
              } catch {
                case _: Throwable => throw scalaReflectionException
              }
          }

        case _ =>
          try {
            reflectMapPartitionsWithIndexInternalV2(rdd, internalRowToUnsafeRowWithIndex)
          } catch {
            case _: Throwable =>
              try {
                reflectMapPartitionsWithIndexInternalV1(rdd, internalRowToUnsafeRowWithIndex)
              } catch {
                case _: Throwable => throw scalaReflectionException
              }
          }
      }

      invokeMapPartitionsWithIndexInternal(version, method, rdd, internalRowToUnsafeRowWithIndex)
    }
  }
}
