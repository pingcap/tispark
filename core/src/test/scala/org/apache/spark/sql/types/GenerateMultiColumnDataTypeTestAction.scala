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

import org.apache.spark.sql.BaseTestGenerationSpec
import org.apache.spark.sql.test.generator.DataType.{getBaseType, getTypeName, DECIMAL, ReflectedDataType}
import org.apache.spark.sql.test.generator.{Data, Index, Schema}
import org.apache.spark.sql.test.generator.TestDataGenerator.{getDecimal, getLength, isCharOrBinary, isVarString, randomDataGenerator, schemaGenerator}

trait GenerateMultiColumnDataTypeTestAction
    extends MultiColumnDataTypeTestSpec
    with BaseTestGenerationSpec
    with DataTypeTestDir {

  override val rowCount = 50

  private def toString(dataTypes: Seq[String]): String = Math.abs(dataTypes.hashCode()).toString

  override def getTableName(dataTypes: String*): String = s"test_${toString(dataTypes)}"

  override def getTableNameWithDesc(desc: String, dataTypes: String*): String =
    s"test_${desc}_${toString(dataTypes)}"

  def genSchema(tableName: String,
                dataTypesWithDescription: List[(ReflectedDataType, String, String)]): Schema = {
    schemaGenerator(
      database,
      tableName,
      r,
      dataTypesWithDescription,
      List.empty[Index]
    )
  }

  def genData(schema: Schema): Data = randomDataGenerator(schema, rowCount, dataTypeTestDir, r)

  def setTiFlashReplicaByConfig(data: Data): Unit = {
    canTestTiFlash = enableTiFlashTest
    data.setTiFLashReplica(canTestTiFlash)
    data.save()
  }

  def init(tableName: String): Unit = {
    val dataTypesWithDescription = dataTypes.map { dataType =>
      val len = getTypeLength(dataType)
      (dataType, len, "")
    }
    val schema = genSchema(tableName, dataTypesWithDescription)
    val data = genData(schema)
    setTiFlashReplicaByConfig(data)
  }

  def loadTestData(tableName: String): Unit

  def test(): Unit = {
    cols = dataTypes
    val tableName = getTableName(dataTypes.map(getTypeName): _*)
    init(tableName)
    loadTestData(tableName)
    if (canTestTiFlash) {
      // sleep for some time to wait for TiFlash syncing
      Thread.sleep(30 * 1000)
    }
  }
}
