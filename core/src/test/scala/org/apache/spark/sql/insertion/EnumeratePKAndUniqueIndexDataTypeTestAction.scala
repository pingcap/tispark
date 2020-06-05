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

trait EnumeratePKAndUniqueIndexDataTypeTestAction extends BaseEnumerateDataTypesTestSpec {
  override def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[List[Index]] = {
    val pkIdxList = genPk(dataTypes, r)
    val uniqueIdxList = genUniqueIndex(dataTypes, r)
    val constraints = scala.collection.mutable.ListBuffer.empty[List[Index]]
    for (i <- pkIdxList.indices) {
      val tmpIdxList = scala.collection.mutable.ListBuffer.empty[Index]
      for (j <- uniqueIdxList.indices) {
        tmpIdxList += pkIdxList(i)
        tmpIdxList += uniqueIdxList(j)
      }
      constraints += tmpIdxList.toList
    }
    constraints.toList
  }

  private def genPk(dataTypes: List[ReflectedDataType], r: Random): List[Index] = {
    val size = dataTypes.length
    val keyList = scala.collection.mutable.ListBuffer.empty[PrimaryKey]
    for (i <- 0 until size) {
      // we add extra one to the column id since 1 is reserved to primary key
      val pkCol = if (isStringType(dataTypes(i))) {
        PrefixColumn(i + 1, r.nextInt(4) + 2) :: Nil
      } else {
        DefaultColumn(i + 1) :: Nil
      }
      keyList += PrimaryKey(pkCol)
    }
    keyList.toList
  }

  private def genUniqueIndex(dataTypes: List[ReflectedDataType], r: Random): List[Index] = {
    val size = dataTypes.length
    // the first step is generate all possible keys
    val keyList = scala.collection.mutable.ListBuffer.empty[Key]
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

        keyList += Key(indexColumnList.toList)
      }
    }

    keyList.toList
  }
}
