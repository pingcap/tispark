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

import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator.{checkUnique, getLength, isBinaryCharset, isCharCharset, isNumeric}

import scala.collection.mutable
import scala.util.Random

case class ValueGenerator(dataType: ReflectedDataType,
                          M: Long = -1,
                          D: Int = -1,
                          nullable: Boolean = true,
                          isUnsigned: Boolean = false,
                          noDefault: Boolean = false,
                          default: Any = null,
                          isPrimaryKey: Boolean = false,
                          isUnique: Boolean = false) {

  private val flag: Int = {
    import com.pingcap.tikv.types.DataType._
    var ret = getBaseFlag(dataType)
    if (isPrimaryKey) {
      ret |= PriKeyFlag
      ret |= NotNullFlag
    }
    if (isUnique) {
      ret |= UniqueKeyFlag
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

  private val generateUnique = isPrimaryKey || isUnique

  import com.pingcap.tikv.meta.Collation._
  val tiDataType: TiDataType = getType(dataType, flag, M, D, "", DEF_COLLATION_CODE)

  val rangeSize: Long = dataType match {
    case BIT       => 1 << tiDataType.getLength.toInt
    case BOOLEAN   => 1 << 1
    case TINYINT   => 1 << 8
    case SMALLINT  => 1 << 16
    case MEDIUMINT => 1 << 24
    case INT       => 1L << 32
    // just treat the range size as infinity, the value is meaningless
    case _ => Long.MaxValue
  }

  private var generatedRandomValues: List[Any] = List.empty[Any]
  private var curPos = 0

  ////////////////// Calculate Type Bound //////////////////
  private val lowerBound: Any = {
    if (tiDataType.isUnsigned) {
      dataType match {
        case TINYINT | SMALLINT | MEDIUMINT | INT | BIGINT => 0L
        case _                                             => null
      }
    } else {
      dataType match {
        case TINYINT | SMALLINT | MEDIUMINT | INT | BIGINT => tiDataType.signedLowerBound()
        case _                                             => null
      }
    }
  }

  private val upperBound: Any = {
    if (tiDataType.isUnsigned) {
      dataType match {
        case TINYINT | SMALLINT | MEDIUMINT | INT => tiDataType.unsignedUpperBound()
        case BIGINT                               => toUnsignedBigInt(tiDataType.unsignedUpperBound())
        case _                                    => null
      }
    } else {
      dataType match {
        case TINYINT | SMALLINT | MEDIUMINT | INT | BIGINT => tiDataType.signedUpperBound()
        case _                                             => null
      }
    }
  }

  private val specialBound: List[Any] = {
    val list: List[Any] = dataType match {
      case BIT                                                                     => List(Array[Byte]())
      case TINYINT | SMALLINT | MEDIUMINT | INT | BIGINT if !tiDataType.isUnsigned => List(-1L)
      case _ if isCharCharset(dataType)                                            => List("")
      case _ if isBinaryCharset(dataType)                                          => List(Array[Byte]())
      case _                                                                       => List.empty[String]
    }
    if (lowerBound != null && upperBound != null) {
      list ::: List(lowerBound, upperBound)
    } else {
      list
    }
  }

  def toUnsignedBigInt(l: Long): BigInt = BigInt.long2bigInt(l) - BigInt.long2bigInt(Long.MinValue)

  ////////////////// Generate Random Value //////////////////
  def randomNull(r: Random): Boolean = {
    // 5% of non-null data be null
    !tiDataType.isNotNull && r.nextInt(20) == 0
  }

  def randomUniqueValue(r: Random, set: mutable.Set[Any]): Any = {
    while (true) {
      val value = randomValue(r)
      if (checkUnique(value, set)) {
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
        case BIGINT    => toUnsignedBigInt(r.nextLong())
        case FLOAT     => Math.abs(r.nextFloat())
        case DOUBLE    => Math.abs(r.nextDouble())
        case DECIMAL =>
          val len = getLength(tiDataType)
          val decimal = if (tiDataType.isDecimalUnSpecified) 0 else tiDataType.getDecimal
          (BigDecimal.apply(Math.abs(r.nextLong()) % Math.pow(10, len)) / BigDecimal.apply(
            Math.pow(10, decimal)
          )).bigDecimal
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
          )).bigDecimal
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

  // pre-generate n random values
  def preGenerateRandomValues(r: Random, n: Long): Unit = {
    if (n <= 1e6) {
      generatedRandomValues = if (generateUnique) {
        assert(n <= rangeSize, "random generator cannot generate unique value less than available")
        val set: mutable.Set[Any] = mutable.HashSet.empty[Any]
        set += specialBound.map(TestDataGenerator.hash)
        (0L until n - specialBound.size).map { _ =>
          randomUniqueValue(r, set)
        }.toList ++ specialBound
      } else {
        (0L until n - specialBound.size).map { _ =>
          randomValue(r)
        }.toList ++ specialBound
      }
      assert(generatedRandomValues.size == n)
      curPos = 0
    }
  }

  ////////////////// Iterator //////////////////
  def next(r: Random): Any = {
    if (randomNull(r)) {
      null
    } else {
      if (generatedRandomValues.isEmpty) {
        if (generateUnique) {
          val set: mutable.Set[Any] = mutable.HashSet.empty[Any]
          randomUniqueValue(r, set)
        } else {
          randomValue(r)
        }
      } else {
        next
      }
    }
  }

  def hasNext: Boolean = curPos < generatedRandomValues.size

  def next: Any = {
    assert(
      generatedRandomValues.nonEmpty,
      "Values not pre-generated, please generate values first to use next()"
    )
    assert(
      hasNext || !generateUnique,
      s"Generated random values(${generatedRandomValues.size}) is less than needed(${curPos + 1})."
    )
    if (!hasNext) {
      // reuse previous generated data
      curPos = 0
    }
    curPos += 1
    generatedRandomValues(curPos - 1)
  }

  def reset(): Unit = {
    curPos = 0
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
    val uniqueString = if (isUnique) " unique" else ""
    s"$unsignedString$nullString$uniqueString$defaultString"
  }

  override def toString: String = {
    s"$dataType$typeDescString$descString"
  }
}
