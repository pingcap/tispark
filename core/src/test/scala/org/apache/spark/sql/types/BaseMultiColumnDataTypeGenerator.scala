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

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.test.generator.DataType.{getTypeName, ReflectedDataType}

case class BaseMultiColumnDataTypeGenerator(dataTypes: List[ReflectedDataType],
                                            unsignedDataTypes: List[ReflectedDataType],
                                            dataTypeTestDir: String,
                                            database: String,
                                            testDesc: String)
    extends BaseTiSparkTest
    with GenerateMultiColumnDataTypeTestAction {
  def loadTestData(dataTypes: List[ReflectedDataType]): Unit = {
    val tableName = getTableName(dataTypes.map(getTypeName): _*)
    logger.info(s"${preDescription}Test $tableName - $testDesc")
    loadSQLFile(dataTypeTestDir, tableName)
  }
}
