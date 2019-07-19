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

  def toOutput(value: Any): String = value match {
    case null       => null
    case _: Boolean => value.toString
    case _: Number  => value.toString
    case _: Array[Byte] =>
      s"X\'${value
        .asInstanceOf[Array[Byte]]
        .map { b =>
          String.format("%02x", new java.lang.Byte(b))
        }
        .mkString}\'"
    case _: Array[Boolean] =>
      s"b\'${value
        .asInstanceOf[Array[Boolean]]
        .map {
          case true  => "1"
          case false => "0"
        }
        .mkString}\'"
    case _ => s"\'$value\'"
  }

  private val sql = s"drop table if exists `$database`.`$table`;\n" +
    s"${schema.toString};\n" +
    s"insert into `$database`.`$table` values $text;"

  def save(): Unit = {
    import java.io._
    // FileWriter
    val path = getClass.getResource("/")
    val file = new File(path.getPath + "/../../src/test/resources/" + fileName)
    file.getParentFile.mkdirs()
    file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(sql)
    bw.close()
  }

  override def toString: String =
    schema.toString + "\n" + data
      .map { row: TiRow =>
        row.toString
      }
      .mkString("{\n\t", "\n\t", "\n}")
}
