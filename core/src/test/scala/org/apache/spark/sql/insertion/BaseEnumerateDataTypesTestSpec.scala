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

import org.apache.spark.sql.BaseTestGenerationSpec
import org.apache.spark.sql.test.generator.DataType.ReflectedDataType
import org.apache.spark.sql.test.generator.TestDataGenerator._
import org.apache.spark.sql.test.generator.{Index, Schema}
import org.apache.spark.sql.types.MultiColumnDataTypeTestSpec

import scala.util.Random

trait BaseEnumerateDataTypesTestSpec
    extends MultiColumnDataTypeTestSpec
    with BaseTestGenerationSpec {
  override def rowCount = 50

  def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[List[Index]]

  // this only generate schema with one unique index
  def genSchema(dataTypes: List[ReflectedDataType], tablePrefix: String): List[Schema] = {
    val indices = genIndex(dataTypes, r)

    val dataTypesWithDescription = dataTypes.map { dataType =>
      val len = getTypeLength(dataType)
      if (isNumeric(dataType)) {
        (dataType, len, "not null")
      } else {
        (dataType, len, "")
      }
    }

    indices.zipWithIndex.map { index =>
      schemaGenerator(
        dbName,
        // table name
        tablePrefix + index._2,
        r,
        dataTypesWithDescription,
        // constraint
        index._1)
    }
  }

  // we are not using below function, we probably need decouple the logic.
  override def getTableName(dataTypes: String*): String = ???

  override def getTableNameWithDesc(desc: String, dataTypes: String*): String = ???

  override def getIndexName(dataTypes: String*): String = ???

  private def toString(dataTypes: Seq[String]): String = dataTypes.mkString("[", ",", "]")
}
