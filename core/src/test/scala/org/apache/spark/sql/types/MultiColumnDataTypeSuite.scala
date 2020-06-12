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

import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator._

class MultiColumnDataTypeSuite
    extends MultiColumnDataTypeTest
    with RunMultiColumnDataTypeTestAction {
  override val generator: BaseMultiColumnDataTypeGenerator = BaseMultiColumnDataTypeGenerator(
    dataTypes,
    unsignedDataTypes,
    dataTypeTestDir,
    dbName,
    testDesc)
  override def dataTypes: List[ReflectedDataType] = numeric ::: stringType
  override def unsignedDataTypes: List[ReflectedDataType] = numeric
  override def dataTypeTestDir: String = "multi-column-dataType-test"
  override def dbName: String = "multi_column_data_type_test"
  override def testDesc: String = "Base test for multi-column data types"

  def startTest(dataTypes: List[ReflectedDataType]): Unit = {
    val typeNames = dataTypes.map(getTypeName)
    val tblName = generator.getTableName(typeNames: _*)
    val columnNames = typeNames.zipWithIndex.map { x =>
      generator.getColumnNameByOffset(x._2)
    }
    for (i <- columnNames.indices) {
      val col1 = columnNames(i)
      for (j <- columnNames.indices) {
        val col2 = columnNames(j)
        val dataType2 = dataTypes(j)
        simpleSelect(dbName, tblName, col1, col2, dataType2)
      }
    }
  }

  def check(): Unit = {
    if (generateData) {
      generator.test()
    }
  }

  override def test(): Unit = {
    startTest(dataTypes)
  }

  check()
  test()
}
