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

import org.apache.spark.sql.test.generator.TestDataGenerator.TiRow

/**
 * Data with corresponding schema
 *
 * @param schema generated schema
 * @param data list of [[com.pingcap.tikv.row.Row]]
 * @param directory relative path where the data should be stored
 */
case class Data(schema: Schema, data: List[TiRow], directory: String) {
  private val database = schema.database
  private val table = schema.tableName
  private val fileName = s"$directory/$table.sql"
  private val text =
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
  private val sql = s"CREATE DATABASE IF NOT EXISTS `$database`;\n" +
    s"DROP TABLE IF EXISTS `$database`.`$table`;\n" +
    s"${schema.toString};\n" +
    s"INSERT INTO `$database`.`$table` VALUES $text;\n"
  private val tiflash_sql = s"ALTER TABLE `$database`.`$table` SET TIFLASH REPLICA 1"
  private var hasTiFlashReplica = false

  def setTiFLashReplica(has: Boolean): Unit = {
    hasTiFlashReplica = has
  }

  def toOutput(value: Any): String =
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

  def save(): Unit = {
    import java.io._
    // FileWriter
    val path = getClass.getResource("/")
    val file = new File(path.getPath + fileName)
    file.getParentFile.mkdirs()
    file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(sql)
    if (hasTiFlashReplica) {
      bw.write(tiflash_sql)
    }
    bw.close()
  }

  override def toString: String =
    schema.toString + "\n" + data
      .map { row: TiRow =>
        row.toString
      }
      .mkString("{\n\t", "\n\t", "\n}")
}
