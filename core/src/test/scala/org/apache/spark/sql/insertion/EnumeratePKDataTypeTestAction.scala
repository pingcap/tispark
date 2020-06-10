/*
 * Copyright 2020 PingCAP, Inc.
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

package org.apache.spark.sql.insertion

import org.apache.spark.sql.test.generator.DataType.ReflectedDataType
import org.apache.spark.sql.test.generator.TestDataGenerator.isStringType
import org.apache.spark.sql.test.generator.{DefaultColumn, Index, PrefixColumn, PrimaryKey}

import scala.util.Random

trait EnumeratePKDataTypeTestAction extends BaseEnumerateDataTypesTestSpec {
  override def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[List[Index]] = {
    val size = dataTypes.length
    val keyList = scala.collection.mutable.ListBuffer.empty[List[PrimaryKey]]
    for (i <- 0 until size) {
      // we add extra one to the column id since 1 is reserved to primary key
      val pkCol = if (isStringType(dataTypes(i))) {
        PrefixColumn(i + 1, r.nextInt(4) + 2) :: Nil
      } else {
        DefaultColumn(i + 1) :: Nil
      }
      keyList += PrimaryKey(pkCol) :: Nil
    }
    keyList.toList
  }
}
