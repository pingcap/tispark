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

import org.apache.spark.sql.test.generator.DataType.ReflectedDataType
import org.apache.spark.sql.test.generator.TestDataGenerator.{isStringType, schemaGenerator}
import org.apache.spark.sql.test.generator._
import org.apache.spark.sql.types.GenerateUnitDataTypeTestAction

import scala.util.Random

trait GeneratePKDataTypeTestAction extends GenerateUnitDataTypeTestAction {
  override def genSchema(
      dataType: ReflectedDataType,
      tableName: String,
      len: String,
      desc: String): Schema = {
    val index = genIndex(dataType, r, len)
    schemaGenerator(dbName, tableName, r, List((dataType, len, desc)), index)
  }

  private def genIndex(dataType: ReflectedDataType, r: Random, len: String): List[Index] = {
    if (isStringType(dataType)) {
      List(PrimaryKey(List(PrefixColumn(1, r.nextInt(4) + 2))))
    } else {
      List(PrimaryKey(List(DefaultColumn(1))))
    }
  }
}
