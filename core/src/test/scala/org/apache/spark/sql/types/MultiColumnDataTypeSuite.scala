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
  val dataTypes: List[ReflectedDataType] = numeric ::: stringType
  val unsignedDataTypes: List[ReflectedDataType] = numeric
  val dataTypeTestDir: String = "multi-column-dataType-test"
  val database: String = "multi_column_data_type_test"
  val testDesc: String = "Base test for multi-column data types"

  def startTest(dataTypes: List[ReflectedDataType]): Unit = {
    simpleSelect(database, dataTypes: _*)
  }

  def check(): Unit = {
    if (generateData) {
      BaseMultiColumnDataTypeGenerator(
        dataTypes,
        unsignedDataTypes,
        dataTypeTestDir,
        database,
        testDesc
      ).test()
    }
  }

  check()
  test()
}
