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

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.insertion.BaseEnumerateDataTypesTestSpec
import org.apache.spark.sql.test.generator.DataType.{BIT, BOOLEAN, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator._
import org.apache.spark.sql.test.generator._

import scala.util.Random

trait ClusteredIndexTest extends BaseTiSparkTest with BaseEnumerateDataTypesTestSpec {
  protected val testDataTypes: List[ReflectedDataType] = baseDataTypes

  protected val tablePrefix: String = "clustered"

  override def dbName: String = "tispark_test"

  override def rowCount = 10

  override def dataTypes: List[ReflectedDataType] = ???

  override def unsignedDataTypes: List[ReflectedDataType] = ???

  override def testDesc: String = ???

  override def test(): Unit = ???

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[List[Index]] = {
    val size = dataTypes.length
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
        pkCol = if (isStringType(dataTypes(i))) {
          PrefixColumn(i + 1, r.nextInt(4) + 2) :: pkCol
        } else {
          DefaultColumn(i + 1) :: pkCol
        }
      }

      keys1 = PrimaryKey(pkCol) :: keys1
      keys2 = PrimaryKey(pkCol) :: keys2
    }

    {
      val keyCol = if (isStringType(dataTypes(uniqueKey))) {
        PrefixColumn(uniqueKey + 1, r.nextInt(4) + 2) :: Nil
      } else {
        DefaultColumn(uniqueKey + 1) :: Nil
      }

      keys1 = UniqueKey(keyCol) :: keys1
      keys2 = Key(keyCol) :: keys2
    }

    List(keys1, keys2)
  }

  protected def test(schema: Schema): Unit = {
    executeTiDBSQL(s"drop table if exists `$dbName`.`${schema.tableName}`;")
    executeTiDBSQL(schema.toString(isClusteredIndex = true))

    var rc = rowCount
    schema.columnInfo.foreach { columnInfo =>
      if (columnInfo.dataType.equals(BIT) || columnInfo.dataType.equals(BOOLEAN)) {
        rc = 2
      }
    }

    for (insert <- toInsertSQL(schema, generateRandomRows(schema, rc, r))) {
      try {
        executeTiDBSQL(insert)
      } catch {
        case _: Throwable => println("insert fail")
      }
    }

    val sql = s"select * from `${schema.tableName}`"
    spark.sql(s"explain $sql").show(200, false)
    spark.sql(s"$sql").show(200, false)
    runTest(sql, skipJDBC = true)
  }

  private def executeTiDBSQL(sql: String): Unit = {
    println(sql)
    tidbStmt.execute(sql)
  }

  private def toInsertSQL(schema: Schema, data: List[TiRow]): List[String] = {
    data
      .map { row =>
        (0 until row.fieldCount())
          .map { idx =>
            val value = row.get(idx, schema.columnInfo(idx).generator.tiDataType)
            toOutput(value)
          }
          .mkString("(", ",", ")")
      }
      .map { text =>
        s"INSERT INTO `$dbName`.`${schema.tableName}` VALUES $text;"
      }
  }

  private def toOutput(value: Any): String =
    value match {
      case null => null
      case _: Boolean => value.toString
      case _: Number => value.toString
      case arr: Array[Byte] =>
        s"X\'${arr.map { b =>
          f"${new java.lang.Byte(b)}%02x"
        }.mkString}\'"
      case arr: Array[Boolean] =>
        s"b\'${arr.map {
          case true => "1"
          case false => "0"
        }.mkString}\'"
      case ts: java.sql.Timestamp =>
        // convert to Timestamp output with current TimeZone
        val zonedDateTime = ts.toLocalDateTime.atZone(java.util.TimeZone.getDefault.toZoneId)
        val milliseconds = zonedDateTime.toEpochSecond * 1000L + zonedDateTime.getNano / 1000000
        s"\'${new java.sql.Timestamp(milliseconds)}\'"
      case _ => s"\'$value\'"
    }
}
