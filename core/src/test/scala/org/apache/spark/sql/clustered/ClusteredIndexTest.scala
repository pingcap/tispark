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
import org.apache.spark.sql.test.generator.DataType.ReflectedDataType
import org.apache.spark.sql.test.generator.TestDataGenerator._
import org.apache.spark.sql.test.generator._

import scala.util.Random

trait ClusteredIndexTest extends BaseTiSparkTest with BaseEnumerateDataTypesTestSpec {
  protected val testDataTypes: List[ReflectedDataType] = baseDataTypes

  protected val tablePrefix: String = "clustered"

  override def dbName: String = "tispark_test"

  override def rowCount = 1

  override def dataTypes: List[ReflectedDataType] = ???

  override def unsignedDataTypes: List[ReflectedDataType] = ???

  override def testDesc: String = ???

  override def test(): Unit = ???

  override def afterAll(): Unit = {
    executeTiDBSQL("SET tidb_enable_clustered_index = 0;")
    super.afterAll()
  }

  override def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[List[Index]] = {
    val size = dataTypes.length
    var keys: List[Index] = Nil

    if (size <= 2) {
      return List(keys)
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

      keys = PrimaryKey(pkCol) :: keys
    }

    {
      val ukCol = if (isStringType(dataTypes(uniqueKey))) {
        PrefixColumn(uniqueKey + 1, r.nextInt(4) + 2) :: Nil
      } else {
        DefaultColumn(uniqueKey + 1) :: Nil
      }

      keys = UniqueKey(ukCol) :: keys
    }
    List(keys)
  }

  protected def test(schema: Schema): Unit = {
    executeTiDBSQL("SET tidb_enable_clustered_index = 1;")
    executeTiDBSQL(s"drop table if exists `$dbName`.`${schema.tableName}`;")
    executeTiDBSQL(schema.toString)

    val insert = toInsertSQL(schema, generateRandomRows(schema, rowCount, r)) + ";"
    executeTiDBSQL(insert)

    val sql = s"select * from `${schema.tableName}`"
    spark.sql(s"explain $sql").show(200, false)
    spark.sql(s"$sql").show(200, false)
    runTest(sql, skipJDBC = true)
  }

  private def executeTiDBSQL(sql: String): Unit = {
    println(sql)
    tidbStmt.execute(sql)
  }

  private def toInsertSQL(schema: Schema, data: List[TiRow]): String = {
    val text =
      data
        .map { row =>
          (0 until row.fieldCount())
            .map { idx =>
              val value = row.get(idx, schema.columnInfo(idx).generator.tiDataType)
              toOutput(value)
            }
            .mkString("(", ",", ")")
        }
        .mkString(",")
    s"INSERT INTO `$dbName`.`${schema.tableName}` VALUES $text;\n"
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
