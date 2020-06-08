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

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.test.generator.DataType.ReflectedDataType
import org.apache.spark.sql.types.UnitDataTypeTestSpec

case class DataTypePKGenerator(
    dataTypes: List[ReflectedDataType],
    unsignedDataTypes: List[ReflectedDataType],
    dataTypeTestDir: String,
    dbName: String,
    testDesc: String)
    extends BaseTiSparkTest
    with GeneratePKDataTypeTestAction {
  def loadTestData(typeName: String): Unit = {
    logger.info(s"${preDescription}Test $typeName - $testDesc")
    loadSQLFile(dataTypeTestDir, getTableName(typeName))
  }

  def loadUnsignedTestData(typeName: String): Unit = {
    logger.info(s"${preDescription}Test $extraDesc $typeName - $testDesc")
    loadSQLFile(dataTypeTestDir, getTableNameWithDesc(extraDesc, typeName))
  }
}

object DataTypePKGenerator {
  def apply(u: UnitDataTypeTestSpec): GeneratePKDataTypeTestAction = {
    DataTypePKGenerator(u.dataTypes, u.unsignedDataTypes, u.dataTypeTestDir, u.dbName, u.testDesc)
  }
}
