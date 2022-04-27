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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.types.pk

import com.pingcap.tispark.test.generator.DataGenerator.{baseDataTypes, isNumeric, isStringType}
import com.pingcap.tispark.test.generator.DataType._
import com.pingcap.tispark.test.generator._
import org.apache.spark.sql.types.BaseRandomDataTypeTest

import scala.collection.mutable
import scala.util.Random

trait MultiColumnPKDataTypeSuites extends BaseRandomDataTypeTest {
  override protected val database: String = "multi_column_pk_data_type_test"

  private val dataTypes: List[ReflectedDataType] = baseDataTypes

  private val testDesc: String = "Base test for multi-column pk data types"

  private val dataTypesWithDesc = dataTypes.map { d =>
    genDescription(d, NullableType.Nullable)
  }

  override protected def genIndex(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      r: Random): List[List[Index]] = {
    assert(
      dataTypesWithDesc.lengthCompare(2) >= 0,
      "column size should be at least 2 for multi-column tests")
    val result: mutable.ListBuffer[IndexColumn] = new mutable.ListBuffer[IndexColumn]()
    for (i <- 0 until 2) {
      val d = dataTypesWithDesc(i)._1
      if (isStringType(d)) {
        result += PrefixColumn(i + 1, r.nextInt(4) + 2)
      } else {
        result += DefaultColumn(i + 1)
      }
    }
    List(List(PrimaryKey(result.toList)))
  }

  protected val tests: Map[Int, Seq[(Int, Int)]] = {
    val size = dataTypes.size - 1
    dataTypes.indices
    // filter i<j rather than i!=j to exclude the same tuple (ignore order)
    // The test check if it fetch the correct data. The order of col in pk won't affect it
      .flatten { i =>
        dataTypes.indices
          .filter { j =>
            i < j
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

  protected def getId: Int = getClass.getName.substring(getClass.getName.length - 2).toInt

  private def startTest(schemaAndDataList: List[SchemaAndData], i: Int, j: Int): Unit = {
    test(s"$testDesc - ${getTypeName(dataTypes(i))} ${getTypeName(dataTypes(j))}") {
      schemaAndDataList.foreach { schemaAndData =>
        loadToDB(schemaAndData)

        val dt = List(dataTypes(i), dataTypes(j)) ++ dataTypes
        val database = schemaAndData.schema.database
        val tableName = schemaAndData.schema.tableName
        val columnNames = schemaAndData.schema.columnNames
        for (u <- dt.indices) {
          val col1 = columnNames(u)
          for (v <- u + 1 until dt.size) {
            val col2 = columnNames(v)
            val dataType = dt(v)
            simpleSelect(database, tableName, col1, col2, dataType)
          }
        }
      }
    }
  }

  protected def generateTestCases(): Unit = {
    currentTest.foreach {
      case (i, j) =>
        val cols = List(
          genDescription(dataTypes(i), NullableType.NotNullable),
          genDescription(dataTypes(j), NullableType.NotNullable)) ++ dataTypesWithDesc
        val schemaAndDataList =
          genSchemaAndData(rowCount, cols, database, hasTiFlashReplica = enableTiFlashTest)
        startTest(schemaAndDataList, i, j)
    }
  }
}
