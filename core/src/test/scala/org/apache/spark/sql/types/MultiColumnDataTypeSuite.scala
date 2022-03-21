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

package org.apache.spark.sql.types

import com.pingcap.tispark.test.generator.DataGenerator._
import com.pingcap.tispark.test.generator.DataType.ReflectedDataType
import com.pingcap.tispark.test.generator.{NullableType, SchemaAndData}

class MultiColumnDataTypeSuite extends BaseRandomDataTypeTest {
  override protected def rowCount: Int = 50

  private val dataTypes: List[ReflectedDataType] = numeric ::: stringType

  override protected val database: String = "multi_column_data_type_test"

  private def startTest(schemaAndDataList: List[SchemaAndData]): Unit = {
    test("Test multi-column data types") {
      schemaAndDataList.foreach { schemaAndData =>
        loadToDB(schemaAndData)

        val dbName = schemaAndData.schema.database
        val tblName = schemaAndData.schema.tableName
        val columnNames = schemaAndData.schema.columnNames

        for (i <- columnNames.indices) {
          val col1 = columnNames(i)
          for (j <- columnNames.indices) {
            val col2 = columnNames(j)
            val dataType2 = dataTypes(j)
            simpleSelect(dbName, tblName, col1, col2, dataType2)
          }
        }
      }
    }
  }

  private def generateTestCases(): Unit = {
    val dataTypesWithDesc: List[(ReflectedDataType, String, String)] = dataTypes.map {
      genDescription(_, NullableType.Nullable)
    }
    val schemaAndDataList = genSchemaAndData(
      rowCount,
      dataTypesWithDesc,
      database,
      hasTiFlashReplica = enableTiFlashTest)
    startTest(schemaAndDataList)
  }

  generateTestCases()
}
