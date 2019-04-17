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

import java.lang.reflect.Method

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import scala.reflect.ClassTag

/**
 * ReflectionUtil is designed to reflect methods which differ across
 * different Spark versions. Compatibility issues should be solved by
 * reflections in future.
 */
object ReflectionUtil {
  val spark_version: String = org.apache.spark.SPARK_VERSION

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
  private val mapPartitionsWithIndexInternal: Method = spark_version match {
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
              .format(spark_version)
          )
      }
  }

  case class ReflectionMapPartitionWithIndexInternal(
    rdd: RDD[InternalRow],
    internalRowToUnsafeRowWithIndex: (Int, Iterator[InternalRow]) => Iterator[UnsafeRow]
  ) {
    def invoke(): RDD[InternalRow] =
      spark_version match {
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
}
