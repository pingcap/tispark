/*
 * Copyright 2021 PingCAP, Inc.
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

package org.apache.spark.sql.clustered

import com.pingcap.tispark.test.generator.DataType._
import com.pingcap.tispark.test.generator.DataGenerator._
import com.pingcap.tispark.test.generator._
import org.apache.spark.sql.types.BaseRandomDataTypeTest

import scala.util.Random

trait ClusteredIndexTest extends BaseRandomDataTypeTest {
  protected val testDataTypes1: List[ReflectedDataType] =
    List(BIT, INT, DECIMAL, TIMESTAMP, TEXT, BLOB)

  protected val testDataTypes2: List[ReflectedDataType] =
    List(BOOLEAN, BIGINT, DOUBLE, DATE, VARCHAR)

  override protected val database: String = "clustered_index_test"

  override protected def genIndex(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      r: Random): List[List[Index]] = {
    val size = dataTypesWithDesc.length
    var keys1: List[Index] = Nil
    var keys2: List[Index] = Nil

    if (size <= 2) {
      return List(Nil)
    }

    val primaryKeyList = 0 until size - 2
    val uniqueKey = size - 2

    {
      var pkCol: List[IndexColumn] = Nil
      primaryKeyList.foreach { i =>
        pkCol = if (isStringType(dataTypesWithDesc(i)._1)) {
          PrefixColumn(i + 1, r.nextInt(4) + 2) :: pkCol
        } else {
          DefaultColumn(i + 1) :: pkCol
        }
      }

      keys1 = PrimaryKey(pkCol) :: keys1
      keys2 = PrimaryKey(pkCol) :: keys2
    }

    {
      val keyCol = if (isStringType(dataTypesWithDesc(uniqueKey)._1)) {
        PrefixColumn(uniqueKey + 1, r.nextInt(4) + 2) :: Nil
      } else {
        DefaultColumn(uniqueKey + 1) :: Nil
      }

      keys1 = UniqueKey(keyCol) :: keys1
      keys2 = Key(keyCol) :: keys2
    }

    List(keys1, keys2)
  }

  protected def test(schemaAndData: SchemaAndData): Unit = {
    loadToDB(schemaAndData)

    setCurrentDatabase(schemaAndData.schema.database)
    val sql = s"select * from `${schemaAndData.schema.tableName}`"
    println(sql)
    runTest(sql, skipJDBC = true)
  }
}
