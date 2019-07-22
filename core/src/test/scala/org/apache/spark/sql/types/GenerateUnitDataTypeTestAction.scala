/*
 *
 * Copyright 2017 PingCAP, Inc.
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
import org.apache.spark.sql.test.generator.{Data, Index, Schema}
import org.apache.spark.sql.test.generator.DataType.{getBaseType, getTypeName, DECIMAL, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator.{getDecimal, getLength, isCharOrBinary, isVarString, randomDataGenerator, schemaGenerator}

trait GenerateUnitDataTypeTestAction extends UnitDataTypeTestAction with BaseTestGenerationSpec {

  override val preDescription: String = "Generating Data for "

  def genSchema(dataType: ReflectedDataType,
                tableName: String,
                len: String,
                desc: String): Schema = {
    schemaGenerator(
      database,
      tableName,
      r,
      List(
        (dataType, len, desc)
      ),
      List.empty[Index]
    )
  }

  def genData(schema: Schema): Data = randomDataGenerator(schema, 20, dataTypeTestDir, r)

  def genLen(dataType: ReflectedDataType): String = {
    val baseType = getBaseType(dataType)
    val length = getLength(baseType)
    dataType match {
      case DECIMAL                       => s"$length,${getDecimal(baseType)}"
      case _ if isVarString(dataType)    => s"$length"
      case _ if isCharOrBinary(dataType) => "10"
      case _                             => ""
    }
  }

  def init(): Unit = {
    for (dataType <- dataTypes) {
      val typeName = getTypeName(dataType)
      val len = genLen(dataType)
      val tableName = getTableName(typeName)
      val schema = genSchema(dataType, tableName, len, "")
      val data = genData(schema)
      data.save()
    }
    for (dataType <- unsignedDataTypes) {
      val typeName = getTypeName(dataType)
      val len = genLen(dataType)
      val tableName = getTableName(typeName, extraDesc)
      val schema = genSchema(dataType, tableName, len, extraDesc)
      val data = genData(schema)
      data.save()
    }
  }

  def loadTestData(typeName: String): Unit

  def loadUnsignedTestData(typeName: String): Unit

  def test(): Unit = {
    init()
    for (dataType <- dataTypes) {
      val typeName = getTypeName(dataType)
      loadTestData(typeName)
    }
    for (dataType <- unsignedDataTypes) {
      val typeName = getTypeName(dataType)
      loadUnsignedTestData(typeName)
    }
  }
}
