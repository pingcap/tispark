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

package org.apache.spark.sql.types

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator._

class DataTypeNormalSuite extends BaseTiSparkTest with RunUnitDataTypeTestAction {

  override val dataTypes: List[ReflectedDataType] = numeric
  override val unsignedDataTypes: List[ReflectedDataType] = numeric
  override val dataTypeTestDir = "dataType-test"
  override val database = "data_type_test"
  override val testDesc = "Test for single column data types (and unsigned types)"

  def startTest(typeName: String): Unit = {
    test(s"${preDescription}Test $typeName - $testDesc") {
      simpleSelect(database, typeName)
    }
  }

  def startUnsignedTest(typeName: String): Unit = {
    test(s"${preDescription}Test $extraDesc $typeName - $testDesc") {
      simpleSelect(database, typeName, extraDesc)
    }
  }

  def check(): Unit = {
    SharedSQLContext.init()
    if (generateData) {
      BaseGenerateDataType(dataTypes, unsignedDataTypes, dataTypeTestDir, database, testDesc).test()
    }
  }

  check()
  test()
}
