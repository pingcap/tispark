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

package org.apache.spark.sql.test.generator

import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator.{getLength, isNumeric}

import scala.collection.mutable
import scala.util.Random

case class ValueGenerator(dataType: ReflectedDataType,
                          M: Long = -1,
                          D: Int = -1,
                          nullable: Boolean = true,
                          isUnsigned: Boolean = false,
                          noDefault: Boolean = false,
                          default: Any = null,
                          isPrimaryKey: Boolean = false) {

  private val flag: Int = {
    import com.pingcap.tikv.types.DataType._
    var ret = getBaseFlag(dataType)
    if (isPrimaryKey) {
      ret |= PriKeyFlag
      ret |= NotNullFlag
    }
    if (!nullable) {
      ret |= NotNullFlag
    }
    if (isUnsigned) {
      ret |= UnsignedFlag
    }
    if (noDefault) {
      ret |= NoDefaultValueFlag
    }
    ret
  }

  import com.pingcap.tikv.meta.Collation._
  val tiDataType: TiDataType = getType(dataType, flag, M, D, "", DEF_COLLATION_CODE)

  def randomNull(r: Random): Boolean = {
    // 5% of non-null data be null
    !tiDataType.isNotNull && r.nextInt(20) == 0
  }

  val set: mutable.Set[Any] = mutable.HashSet.empty[Any]

  def randomUniqueValue(r: Random): Any = {
    while (true) {
      val value = randomValue(r)
      val hashedValue = value match {
        case null           => "null"
        case b: Array[Byte] => b.mkString("[", ",", "]")
        case x              => x.toString
      }
      println(value, hashedValue)
      if (!set.apply(hashedValue)) {
        set += hashedValue
        return value
      }
    }
  }

  def randomValue(r: Random): Any = {
    if (tiDataType.isUnsigned) {
      if (!isNumeric(dataType)) {
        throw new IllegalArgumentException("unsigned type is not numeric")
      }
      dataType match {
        case BIT =>
          val bit: Array[Boolean] = new Array[Boolean](tiDataType.getLength.toInt)
          bit.map(_ => r.nextBoolean())
        case BOOLEAN   => r.nextInt(1 << 1)
        case TINYINT   => r.nextInt(1 << 8)
        case SMALLINT  => r.nextInt(1 << 16)
        case MEDIUMINT => r.nextInt(1 << 24)
        case INT       => r.nextInt() + (1L << 31)
        case BIGINT    => BigInt.long2bigInt(r.nextLong()) - BigInt.long2bigInt(Long.MinValue)
        case FLOAT     => Math.abs(r.nextFloat())
        case DOUBLE    => Math.abs(r.nextDouble())
        case DECIMAL =>
          val len = getLength(tiDataType)
          val decimal = if (tiDataType.isDecimalUnSpecified) 0 else tiDataType.getDecimal
          (BigDecimal.apply(Math.abs(r.nextLong()) % Math.pow(10, len)) / BigDecimal.apply(
            Math.pow(10, decimal)
          )).bigDecimal.toPlainString
      }
    } else {
      dataType match {
        case BIT =>
          val bit: Array[Boolean] = new Array[Boolean](tiDataType.getLength.toInt)
          bit.map(_ => r.nextBoolean())
        case BOOLEAN   => r.nextInt(1 << 1)
        case TINYINT   => r.nextInt(1 << 8) - (1 << 7)
        case SMALLINT  => r.nextInt(1 << 16) - (1 << 15)
        case MEDIUMINT => r.nextInt(1 << 24) - (1 << 23)
        case INT       => r.nextInt()
        case BIGINT    => r.nextLong()
        case FLOAT     => r.nextFloat()
        case DOUBLE    => r.nextDouble()
        case DECIMAL =>
          val len = getLength(tiDataType)
          val decimal = if (tiDataType.isDecimalUnSpecified) 0 else tiDataType.getDecimal
          (BigDecimal.apply(r.nextLong() % Math.pow(10, len)) / BigDecimal.apply(
            Math.pow(10, decimal)
          )).bigDecimal.toPlainString
        case VARCHAR   => generateRandomString(r, tiDataType.getLength)
        case VARBINARY => generateRandomBinary(r, tiDataType.getLength)
        case CHAR | TEXT | TINYTEXT | MEDIUMTEXT | LONGTEXT =>
          generateRandomString(r, getRandomLength(dataType, r))
        case BINARY | BLOB | TINYBLOB | MEDIUMBLOB | LONGBLOB =>
          generateRandomBinary(r, getRandomLength(dataType, r))
        case _ => throw new RuntimeException("not supported yet")
      }
    }
  }

  private def getRandomLength(dataType: ReflectedDataType, r: Random): Long = {
    var len = getLength(tiDataType)
    if (len == -1) {
      len = r.nextInt(40) + 10
    }
    len
  }

  private def generateRandomString(r: Random, length: Long, isAlphaNum: Boolean = false): String = {
    val alphaNum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    val s = StringBuilder.newBuilder
    for (_ <- 0L until length) {
      s.append(alphaNum.charAt(Math.abs(r.nextInt()) % alphaNum.length))
    }
    s.mkString
  }

  private def generateRandomBinary(r: Random, length: Long): Array[Byte] = {
    val b: Array[Byte] = new Array[Byte](length.toInt)
    r.nextBytes(b)
    b
  }

  ////////////////// To Description String //////////////////
  private val typeDescString: String = dataType match {
    case BOOLEAN => ""
    case _ =>
      if (M == -1) {
        ""
      } else if (D == -1) {
        s"($M)"
      } else {
        s"($M,$D)"
      }
  }

  private val descString = {
    val nullString = if (!nullable) " not null" else ""
    val defaultString = if (!noDefault) s" default $default" else ""
    val unsignedString = if (isUnsigned) " unsigned" else ""
    s"$unsignedString$nullString$defaultString"
  }

  override def toString: String = {
    s"$dataType$typeDescString$descString"
  }
}
