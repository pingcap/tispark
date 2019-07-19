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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, TiSessionCatalog}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * ReflectionUtil is designed to reflect methods which differ across
 * different Spark versions. Compatibility issues should be solved by
 * reflections in future.
 */
object ReflectionUtil {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  val sparkVersion: String = org.apache.spark.SPARK_VERSION

  val sparkMajorVersion: String = if (sparkVersion.startsWith("2.3")) {
    "2.3"
  } else if (sparkVersion.startsWith("2.4")) {
    "2.4"
  } else {
    ""
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
  private val mapPartitionsWithIndexInternal: Method = sparkVersion match {
    case "2.3.0" | "2.3.1" =>
      classOf[RDD[InternalRow]].getDeclaredMethod(
        "mapPartitionsWithIndexInternal",
        classOf[(Int, Iterator[InternalRow]) => Iterator[UnsafeRow]],
        classOf[Boolean],
        classOf[ClassTag[UnsafeRow]]
      )
    case _ =>
      // Spark version >= 2.3.2
      try {
        classOf[RDD[InternalRow]].getDeclaredMethod(
          "mapPartitionsWithIndexInternal",
          classOf[(Int, Iterator[InternalRow]) => Iterator[UnsafeRow]],
          classOf[Boolean],
          classOf[Boolean],
          classOf[ClassTag[UnsafeRow]]
        )
      } catch {
        case _: Throwable =>
          throw ScalaReflectionException(
            "Cannot find reflection of Method mapPartitionsWithIndexInternal, current Spark version is %s"
              .format(sparkVersion)
          )
      }
  }

  case class ReflectionMapPartitionWithIndexInternal(
    rdd: RDD[InternalRow],
    internalRowToUnsafeRowWithIndex: (Int, Iterator[InternalRow]) => Iterator[UnsafeRow]
  ) {
    def invoke(): RDD[InternalRow] =
      sparkVersion match {
        case "2.3.0" | "2.3.1" =>
          mapPartitionsWithIndexInternal
            .invoke(
              rdd,
              internalRowToUnsafeRowWithIndex,
              Boolean.box(false),
              ClassTag.apply(classOf[UnsafeRow])
            )
            .asInstanceOf[RDD[InternalRow]]
        case _ =>
          mapPartitionsWithIndexInternal
            .invoke(
              rdd,
              internalRowToUnsafeRowWithIndex,
              Boolean.box(false),
              Boolean.box(false),
              ClassTag.apply(classOf[UnsafeRow])
            )
            .asInstanceOf[RDD[InternalRow]]
      }
  }

  lazy val classLoader: URLClassLoader = {
    val tisparkClassUrl = this.getClass.getProtectionDomain.getCodeSource.getLocation
    val tisparkClassPath = new File(tisparkClassUrl.getFile)
    logger.info(s"tispark class url: ${tisparkClassUrl.toString}")

    val sparkWrapperClassURL: URL = if (tisparkClassPath.isDirectory) {
      val classDir = new File(
        s"${tisparkClassPath.getAbsolutePath}/../../../spark-wrapper/spark-$sparkMajorVersion/target/classes/"
      )
      if (!classDir.exists()) {
        throw new Exception(
          "cannot find spark wrapper classes! please compile the spark-wrapper project first!"
        )
      }
      classDir.toURI.toURL
    } else {
      new URL(s"jar:$tisparkClassUrl!/resources/spark-wrapper-spark-$sparkMajorVersion/")
    }
    logger.info(s"spark wrapper class url: ${sparkWrapperClassURL.toString}")

    new URLClassLoader(Array(sparkWrapperClassURL), this.getClass.getClassLoader)
  }

  def newTiDirectExternalCatalog(tiContext: TiContext): ExternalCatalog = {
    val clazz =
      classLoader.loadClass("org.apache.spark.sql.catalyst.catalog.TiDirectExternalCatalog")
    clazz
      .getDeclaredConstructor(classOf[TiContext])
      .newInstance(tiContext)
      .asInstanceOf[ExternalCatalog]
  }

  def newTiCompositeSessionCatalog(tiContext: TiContext): TiSessionCatalog = {
    val clazz =
      classLoader.loadClass("org.apache.spark.sql.catalyst.catalog.TiCompositeSessionCatalog")
    clazz
      .getDeclaredConstructor(classOf[TiContext])
      .newInstance(tiContext)
      .asInstanceOf[TiSessionCatalog]
  }

  def callTiAggregationImplUnapply(
    plan: LogicalPlan
  ): Option[(Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)] = {
    val clazz =
      classLoader.loadClass("org.apache.spark.sql.TiAggregationImpl")
    clazz
      .getDeclaredMethod("unapply", classOf[LogicalPlan])
      .invoke(null, plan)
      .asInstanceOf[Option[
        (Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)
      ]]
  }
}
