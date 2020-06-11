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

package org.apache.spark.sql.test.generator

import java.util

import com.pingcap.tikv.meta.Collation
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder

object DataType extends Enumeration {
  type ReflectedDataType = Value
  type TiDataType = com.pingcap.tikv.types.DataType
  val BIT: Val = Val("bit", com.pingcap.tikv.types.BitType.BIT)
  val BOOLEAN: Val = Val("boolean", com.pingcap.tikv.types.IntegerType.BOOLEAN)
  val TINYINT: Val = Val("tinyint", com.pingcap.tikv.types.IntegerType.TINYINT)
  val SMALLINT: Val = Val("smallint", com.pingcap.tikv.types.IntegerType.SMALLINT)
  val MEDIUMINT: Val = Val("mediumint", com.pingcap.tikv.types.IntegerType.MEDIUMINT)
  val INT: Val = Val("int", com.pingcap.tikv.types.IntegerType.INT)
  val BIGINT: Val = Val("bigint", com.pingcap.tikv.types.IntegerType.BIGINT)
  val DECIMAL: Val = Val("decimal", com.pingcap.tikv.types.DecimalType.DECIMAL)
  val FLOAT: Val = Val("float", com.pingcap.tikv.types.RealType.FLOAT)
  val DOUBLE: Val = Val("double", com.pingcap.tikv.types.RealType.DOUBLE)
  val TIMESTAMP: Val = Val("timestamp", com.pingcap.tikv.types.TimestampType.TIMESTAMP)
  val DATETIME: Val = Val("datetime", com.pingcap.tikv.types.DateTimeType.DATETIME)
  val DATE: Val = Val("date", com.pingcap.tikv.types.DateType.DATE)
  val TIME: Val = Val("time", com.pingcap.tikv.types.TimeType.TIME)
  val YEAR: Val = Val("year", com.pingcap.tikv.types.IntegerType.YEAR)
  val TEXT: Val = Val("text", com.pingcap.tikv.types.BytesType.TEXT)
  val TINYTEXT: Val = Val("tinytext", com.pingcap.tikv.types.BytesType.TINY_BLOB)
  val MEDIUMTEXT: Val = Val("mediumtext", com.pingcap.tikv.types.BytesType.MEDIUM_TEXT)
  val LONGTEXT: Val = Val("longtext", com.pingcap.tikv.types.BytesType.LONG_TEXT)
  val VARCHAR: Val = Val("varchar", com.pingcap.tikv.types.StringType.VARCHAR)
  val CHAR: Val = Val("char", com.pingcap.tikv.types.StringType.CHAR)
  val VARBINARY: Val = Val("varbinary", com.pingcap.tikv.types.StringType.VARCHAR)
  val BINARY: Val = Val("binary", com.pingcap.tikv.types.StringType.CHAR)
  val BLOB: Val = Val("blob", com.pingcap.tikv.types.BytesType.BLOB)
  val TINYBLOB: Val = Val("tinyblob", com.pingcap.tikv.types.BytesType.TINY_BLOB)
  val MEDIUMBLOB: Val = Val("mediumblob", com.pingcap.tikv.types.BytesType.MEDIUM_TEXT)
  val LONGBLOB: Val = Val("longblob", com.pingcap.tikv.types.BytesType.LONG_TEXT)
  val ENUM: Val = Val("enum", com.pingcap.tikv.types.EnumType.ENUM)
  val SET: Val = Val("set", com.pingcap.tikv.types.SetType.SET)
  val JSON: Val = Val("json", com.pingcap.tikv.types.JsonType.JSON)

  def getType(
      dataType: ReflectedDataType,
      flag: Integer,
      len: Long,
      decimal: Integer,
      charset: String,
      collation: Integer): TiDataType =
    dataType.asInstanceOf[Val].getType(flag, len, decimal, charset, collation)

  def getBaseType(dataType: ReflectedDataType): TiDataType =
    dataType.asInstanceOf[Val].getBaseType

  def getTypeName(dataType: ReflectedDataType): String = dataType.asInstanceOf[Val].typeName

  def getBaseFlag(dataType: ReflectedDataType): Int = dataType.asInstanceOf[Val].getBaseFlag

  case class Val(typeName: String, private val baseType: TiDataType) extends super.Val {

    override def toString(): String = typeName

    private[generator] def getType(
        flag: Integer,
        len: Long,
        decimal: Integer,
        charset: String,
        collation: Integer): TiDataType = {
      val constructor = baseType.getClass.getDeclaredConstructor(classOf[InternalTypeHolder])
      constructor.setAccessible(true)
      constructor.newInstance(
        new InternalTypeHolder(
          baseType.getTypeCode,
          flag,
          len,
          decimal,
          charset,
          Collation.translate(collation),
          new util.ArrayList[String]()))
    }

    private[generator] def getBaseFlag: Int = baseType.getFlag

    private[generator] def getBaseType: TiDataType = baseType
  }
}
