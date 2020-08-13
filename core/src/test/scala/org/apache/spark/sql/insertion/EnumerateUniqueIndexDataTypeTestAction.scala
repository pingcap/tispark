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

import org.apache.commons.math3.util.Combinations
import org.apache.spark.sql.test.generator.DataType.ReflectedDataType
import org.apache.spark.sql.test.generator.TestDataGenerator.isStringType
import org.apache.spark.sql.test.generator._

import scala.util.Random

trait EnumerateUniqueIndexDataTypeTestAction extends BaseEnumerateDataTypesTestSpec {
  override def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[List[Index]] = {
    val size = dataTypes.length
    // the first step is generate all possible keys
    val keyList = scala.collection.mutable.ListBuffer.empty[List[UniqueKey]]
    for (i <- 1 until 3) {
      val combination = new Combinations(size, i)
      //(i, size)
      val iterator = combination.iterator()
      while (iterator.hasNext) {
        val intArray = iterator.next()
        val indexColumnList = scala.collection.mutable.ListBuffer.empty[IndexColumn]
        // index may have multiple column
        for (j <- 0 until intArray.length) {
          // we add extra one to the column id since 1 is reserved to primary key
          if (isStringType(dataTypes(intArray(j)))) {
            indexColumnList += PrefixColumn(intArray(j) + 1, r.nextInt(4) + 2)
          } else {
            indexColumnList += DefaultColumn(intArray(j) + 1)
          }
        }

        keyList += UniqueKey(indexColumnList.toList) :: Nil
      }
    }

    keyList.toList
  }
}
