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

package org.apache.spark.sql.types.pk

import org.apache.spark.sql.test.generator.DataType.{BIGINT, INT, ReflectedDataType, getTypeName}
import org.apache.spark.sql.test.generator.TestDataGenerator._
import org.apache.spark.sql.types.{MultiColumnDataTypeTest, RunMultiColumnDataTypeTestAction}

trait MultiColumnPKDataTypeSuites
    extends MultiColumnDataTypeTest
    with RunMultiColumnDataTypeTestAction {
  override val generator: MultiColumnDataTypePKGenerator = MultiColumnDataTypePKGenerator(
    dataTypes,
    unsignedDataTypes,
    dataTypeTestDir,
    dbName,
    testDesc)
  override def dataTypes: List[ReflectedDataType] = baseDataTypes
  override def unsignedDataTypes: List[ReflectedDataType] = List(INT, BIGINT)
  override def dataTypeTestDir: String = "multi-column-dataType-test-pk"
  override def dbName: String = "multi_column_pk_data_type_test"
  override def testDesc: String = "Base test for multi-column pk data types"
  val tests: Map[Int, Seq[(Int, Int)]] = {
    val size = dataTypes.size - 1
    dataTypes.indices
      .flatten { i =>
        dataTypes.indices
          .filter { j =>
            i != j
          }
          .map { j =>
            (i, j)
          }
      }
      .groupBy {
        case (i, j) =>
          (i * size + (if (i > j) j else j - 1)) % 36
      }
      .withDefaultValue(Seq.empty[(Int, Int)])
  }
  def currentTest: Seq[(Int, Int)]

  def getId: Int = getClass.getName.substring(getClass.getName.length - 2).toInt

  override def test(): Unit = {
    currentTest.foreach {
      case (i, j) =>
        check(i, j)
        test(i, j)
    }
  }

  def check(i: Int, j: Int): Unit = {
    if (generateData) {
      generator.test(i, j)
    }
  }

  def test(i: Int, j: Int): Unit = {
    startTest(dataTypes, i, j)
  }

  def startTest(dataTypes: List[ReflectedDataType], i: Int, j: Int): Unit = {
    val dt = List(dataTypes(i), dataTypes(j)) ++ dataTypes
    val tableName = generator.getTableName(dt.map(getTypeName): _*)
    val typeNames = dt.map(getTypeName)
    val columnNames = typeNames.zipWithIndex.map { x =>
      generator.getColumnNameByOffset(x._2)
    }
    for (u <- dt.indices) {
      val col1 = columnNames(u)
      for (v <- u + 1 until dt.size) {
        val col2 = columnNames(v)
        val dataType = dt(v)
        simpleSelect(dbName, tableName, col1, col2, dataType)
      }
    }
  }
}
