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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.test

import com.pingcap.tispark.test.generator.DataGenerator._
import com.pingcap.tispark.test.generator.DataType._
import com.pingcap.tispark.test.generator._

import scala.util.Random

trait RandomTest {
  protected val r: Random

  protected def genIndex(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      r: Random): List[List[Index]] = {
    List(List.empty[Index])
  }

  protected def genSchemaAndData(
      rowCount: Int,
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      database: String,
      isClusteredIndex: Boolean = false,
      hasTiFlashReplica: Boolean = false): List[SchemaAndData] = {
    val schemaList =
      genSchema(dataTypesWithDesc, database, isClusteredIndex, hasTiFlashReplica)
    schemaList.map { schema =>
      SchemaAndData(schema, generateRandomRows(schema, rowCount, r))
    }
  }

  protected def genDescription(
      dataType: ReflectedDataType,
      descriptionType: NullableType.Value,
      desc: String = ""): (ReflectedDataType, String, String) = {
    val len = getTypeLength(dataType)
    descriptionType match {
      case NullableType.NotNullable =>
        (dataType, len, s"$desc not null")
      case NullableType.Nullable =>
        (dataType, len, s"$desc null")
      case NullableType.NumericNotNullable =>
        if (isNumeric(dataType)) {
          (dataType, len, s"$desc not null")
        } else {
          (dataType, len, s"$desc null")
        }
      case NullableType.NotNullablePK =>
        (dataType, len, s"$desc not null primary key")

    }
  }

  // this only generate schema with one unique index
  private def genSchema(
      dataTypesWithDesc: List[(ReflectedDataType, String, String)],
      database: String,
      isClusteredIndex: Boolean = false,
      hasTiFlashReplica: Boolean = false): List[Schema] = {
    val tablePrefix = getTableNamePrefix(dataTypesWithDesc.map(t => getTypeName(t._1)): _*)
    val indices = genIndex(dataTypesWithDesc, r)
    indices.zipWithIndex.map { index =>
      val tableName = if (indices.length > 1) tablePrefix + index._2 else tablePrefix
      val schema = schemaGenerator(
        database,
        tableName,
        r,
        dataTypesWithDesc,
        // constraint
        index._1)
      schema.isClusteredIndex = isClusteredIndex
      schema.hasTiFlashReplica = hasTiFlashReplica
      schema
    }
  }

  private def getTypeLength(dataType: ReflectedDataType): String = {
    val baseType = getBaseType(dataType)
    val length = getLength(baseType)
    dataType match {
      case DECIMAL => s"$length,${getDecimal(baseType)}"
      case _ if isVarString(dataType) => s"$length"
      case _ if isCharOrBinary(dataType) => "10"
      case _ => ""
    }
  }

  private def getTableNamePrefix(dataTypes: String*): String = {
    s"test_${toString(dataTypes)}_${Math.abs(r.nextInt())}"
  }

  private def toString(dataTypes: Seq[String]): String = {
    if (dataTypes.length == 1) {
      dataTypes.head
    } else {
      Math.abs(dataTypes.hashCode()).toString
    }
  }
}
