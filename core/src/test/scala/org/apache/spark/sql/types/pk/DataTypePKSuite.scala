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

package org.apache.spark.sql.types.pk

import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator._
import org.apache.spark.sql.test.generator._
import org.apache.spark.sql.types.DataTypeNormalSuite

import scala.util.Random

class DataTypePKSuite extends DataTypeNormalSuite {

  override val dataTypes: List[ReflectedDataType] = integers ::: decimals ::: doubles ::: stringType
  override val unsignedDataTypes: List[ReflectedDataType] = integers ::: decimals ::: doubles
  override val dataTypeTestDir = "dataType-test-pk"
  override val database = "data_type_test_pk"
  override val testDesc = "Test for single PK column data types (and unsigned types)"

  private def genIndex(dataType: ReflectedDataType, r: Random, len: String): List[Index] = {
    if (isStringType(dataType)) {
      List(PrimaryKey(List(PrefixColumn(1, r.nextInt(4) + 2))))
    } else {
      List(PrimaryKey(List(DefaultColumn(1))))
    }
  }

  override def genSchema(dataType: ReflectedDataType,
                         tableName: String,
                         len: String,
                         desc: String): Schema = {
    val index = genIndex(dataType, r, len)
    schemaGenerator(
      database,
      tableName,
      r,
      List(
        (dataType, len, desc)
      ),
      index
    )
  }

  run()
}
