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

import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator._
import org.apache.spark.sql.types.{BaseDataTypeTest, RunUnitDataTypeTestAction}

class DataTypePKSuite extends BaseDataTypeTest with RunUnitDataTypeTestAction {
  override lazy protected val generator: DataTypePKGenerator =
    DataTypePKGenerator(dataTypes, unsignedDataTypes, dataTypeTestDir, dbName, testDesc)
  override def dataTypes: List[ReflectedDataType] =
    integers ::: decimals ::: doubles ::: stringType
  override def unsignedDataTypes: List[ReflectedDataType] = integers ::: decimals ::: doubles
  override def dataTypeTestDir = "dataType-test-pk"
  override def dbName = "data_type_test_pk"
  override def testDesc = "Test for single PK column data types (and unsigned types)"

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
