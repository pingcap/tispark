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

  def getType(dataType: ReflectedDataType,
              flag: Integer,
              len: Long,
              decimal: Integer,
              charset: String,
              collation: Integer): TiDataType =
    dataType.asInstanceOf[Val].getType(flag, len, decimal, charset, collation)
  def getBaseType(dataType: ReflectedDataType): TiDataType = dataType.asInstanceOf[Val].getBaseType
  def getTypeName(dataType: ReflectedDataType): String = dataType.asInstanceOf[Val].typeName
  def getBaseFlag(dataType: ReflectedDataType): Int = dataType.asInstanceOf[Val].getBaseFlag

  case class Val(typeName: String, private val baseType: TiDataType) extends super.Val {

    private[generator] def getType(flag: Integer,
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
          new util.ArrayList[String]()
        )
      )
    }

    private[generator] def getBaseFlag: Int = baseType.getFlag

    private[generator] def getBaseType: TiDataType = baseType

    override def toString(): String = typeName
  }

  val BIT = Val("bit", com.pingcap.tikv.types.BitType.BIT)
  val BOOLEAN = Val("boolean", com.pingcap.tikv.types.IntegerType.BOOLEAN)
  val TINYINT = Val("tinyint", com.pingcap.tikv.types.IntegerType.TINYINT)
  val SMALLINT = Val("smallint", com.pingcap.tikv.types.IntegerType.SMALLINT)
  val MEDIUMINT = Val("mediumint", com.pingcap.tikv.types.IntegerType.MEDIUMINT)
  val INT = Val("int", com.pingcap.tikv.types.IntegerType.INT)
  val BIGINT = Val("bigint", com.pingcap.tikv.types.IntegerType.BIGINT)
  val DECIMAL = Val("decimal", com.pingcap.tikv.types.DecimalType.DECIMAL)
  val FLOAT = Val("float", com.pingcap.tikv.types.RealType.FLOAT)
  val DOUBLE = Val("double", com.pingcap.tikv.types.RealType.DOUBLE)
  val TIMESTAMP = Val("timestamp", com.pingcap.tikv.types.TimestampType.TIMESTAMP)
  val DATETIME = Val("datetime", com.pingcap.tikv.types.DateTimeType.DATETIME)
  val DATE = Val("date", com.pingcap.tikv.types.DateType.DATE)
  val TIME = Val("time", com.pingcap.tikv.types.TimeType.TIME)
  val YEAR = Val("year", com.pingcap.tikv.types.IntegerType.YEAR)
  val TEXT = Val("text", com.pingcap.tikv.types.BytesType.TEXT)
  val TINYTEXT = Val("tinytext", com.pingcap.tikv.types.BytesType.TINY_BLOB)
  val MEDIUMTEXT = Val("mediumtext", com.pingcap.tikv.types.BytesType.MEDIUM_TEXT)
  val LONGTEXT = Val("longtext", com.pingcap.tikv.types.BytesType.LONG_TEXT)
  val VARCHAR = Val("varchar", com.pingcap.tikv.types.StringType.VARCHAR)
  val CHAR = Val("char", com.pingcap.tikv.types.StringType.CHAR)
  val VARBINARY = Val("varbinary", com.pingcap.tikv.types.StringType.VARCHAR)
  val BINARY = Val("binary", com.pingcap.tikv.types.StringType.CHAR)
  val BLOB = Val("blob", com.pingcap.tikv.types.BytesType.BLOB)
  val TINYBLOB = Val("tinyblob", com.pingcap.tikv.types.BytesType.TINY_BLOB)
  val MEDIUMBLOB = Val("mediumblob", com.pingcap.tikv.types.BytesType.MEDIUM_TEXT)
  val LONGBLOB = Val("longblob", com.pingcap.tikv.types.BytesType.LONG_TEXT)
  val ENUM = Val("enum", com.pingcap.tikv.types.EnumType.ENUM)
  val SET = Val("set", com.pingcap.tikv.types.SetType.SET)
  val JSON = Val("json", com.pingcap.tikv.types.JsonType.JSON)
}
