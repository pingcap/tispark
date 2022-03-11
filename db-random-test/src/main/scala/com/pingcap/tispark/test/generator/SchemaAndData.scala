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

package com.pingcap.tispark.test.generator

import com.pingcap.tispark.test.generator.DataGenerator.TiRow

case class SchemaAndData(schema: Schema, data: List[TiRow]) {
  private val fileName = s"random-test/${schema.database}/${schema.tableName}.sql"

  def getInitSQLList: List[String] = {
    val database = schema.database
    val table = schema.tableName

    var sql = s"CREATE DATABASE IF NOT EXISTS `$database`;" ::
      s"USE `$database`;" ::
      s"DROP TABLE IF EXISTS `$table`;" ::
      s"${schema.toString};" :: Nil

    if (data.nonEmpty) {
      val insert =
        data
          .map { row =>
            (0 until row.fieldCount())
              .map { idx =>
                val value = row.get(idx, schema.columnInfo(idx).generator.tiDataType)
                toOutput(value)
              }
              .mkString("(", ",", ")")
          }
          .map(values => s"INSERT INTO `$table` VALUES $values;")
      sql = sql ::: insert
    }

    if (schema.hasTiFlashReplica) {
      val tiflash_sql = s"ALTER TABLE `$table` SET TIFLASH REPLICA 1"
      sql = sql ::: tiflash_sql :: Nil
    }
    sql
  }

  private def toOutput(value: Any): String = {
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

  def save(): Unit = {
    import java.io._
    val path = getClass.getResource("/")
    val file = new File(path.getPath + fileName)
    file.getParentFile.mkdirs()
    file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(getInitSQLList.mkString("\n"))
    bw.close()
  }
}
