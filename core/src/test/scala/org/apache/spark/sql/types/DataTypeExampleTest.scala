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

import org.apache.spark.sql.test.generator.DataType.{ReflectedDataType, TINYINT}

class DataTypeExampleTest extends BaseDataTypeTest with RunUnitDataTypeTestAction {
  val dataTypes: List[ReflectedDataType] = List(TINYINT)
  val unsignedDataTypes: List[ReflectedDataType] = List(TINYINT)
  val dataTypeTestDir: String = "dataType-test"
  val database: String = "data_type_test_example"
  val testDesc: String = "Base test for data types"

  override lazy protected val generator =
    BaseDataTypeGenerator(dataTypes, unsignedDataTypes, dataTypeTestDir, database, testDesc)

  def startTest(typeName: String): Unit = {
    test(s"Test $typeName - $testDesc") {
      simpleSelect(database, typeName)
    }
  }

  def startUnsignedTest(typeName: String): Unit = {
    test(s"Test $extraDesc $typeName - $testDesc") {
      simpleSelect(database, typeName, extraDesc)
    }
  }

  def check(): Unit = {
    if (generateData) {
      generator.test()
    }
  }

  check()
  test()
}
