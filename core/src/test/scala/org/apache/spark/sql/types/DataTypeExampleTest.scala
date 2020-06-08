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
  override lazy protected val generator: BaseDataTypeGenerator =
    BaseDataTypeGenerator(dataTypes, unsignedDataTypes, dataTypeTestDir, dbName, testDesc)
  override def dataTypes: List[ReflectedDataType] = List(TINYINT)
  override def unsignedDataTypes: List[ReflectedDataType] = List(TINYINT)
  override def dataTypeTestDir: String = "dataType-test"
  override def dbName: String = "data_type_test_example"
  override def testDesc: String = "Base test for data types"

  def startTest(typeName: String): Unit = {
    test(s"Test $typeName - $testDesc") {
      simpleSelect(dbName, typeName)
    }
  }

  def startUnsignedTest(typeName: String): Unit = {
    test(s"Test $extraDesc $typeName - $testDesc") {
      simpleSelect(dbName, typeName, extraDesc)
    }
  }

  def check(): Unit = {
    if (generateData) {
      generator.test()
    }
  }

  test()
}
