/*
 *
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
 *
 */

package org.apache.spark.sql.types

import org.apache.spark.sql.{BaseTestGenerationSpec, BaseTiSparkTest}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator._

import scala.collection.mutable

trait MultiColumnDataTypeTest extends BaseTiSparkTest {

  protected val generator: BaseTestGenerationSpec

  private val cmps: List[String] = List(">", "<")
  private val eqs: List[String] = List("=", "<>")

  implicit class C[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X, Y)] = for { x <- xs; y <- ys } yield (x, y)
  }

  def getOperations(dataType: ReflectedDataType): List[(String, String)] =
    List(("is", "null")) ++ {
      (cmps ++ eqs) cross {
        dataType match {
          case TINYINT                     => List("1", "0")
          case _ if isNumeric(dataType)    => List("1", "2333")
          case _ if isStringType(dataType) => List("\'PingCAP\'", "\'\'")
          case _                           => List.empty[String]
        }
      }
    } ++ {
      eqs cross {
        dataType match {
          case BOOLEAN => List("false", "true")
          case _       => List.empty[String]
        }
      }
    }

  def simpleSelect(dbName: String, dataTypes: ReflectedDataType*): Unit = {
    for (i <- 5 until 7) { //dataTypes.indices) {
      for (j <- 5 until 7) { //dataTypes.indices) {
        val dt = List(dataTypes(i), dataTypes(j)) ++ dataTypes
        val tableName = generator.getTableName(dt.map(getTypeName): _*)
        val typeNames = dt.map(getTypeName)
        val columnNames = typeNames.zipWithIndex.map { x =>
          generator.getColumnName(x._1)
        }
        for (u <- columnNames.indices) {
          for (v <- u + 1 until columnNames.size) {
            val col = columnNames(v)
            val types = dt(v)
            for ((op, value) <- getOperations(types)) {
              val query = s"select ${columnNames(u)} from $tableName where $col $op $value"
              test((u, v) + query) {
                setCurrentDatabase(dbName)
                runTest(query)
              }
            }
          }
        }
      }
    }
  }

  // initialize test framework
  SharedSQLContext.init()
}
