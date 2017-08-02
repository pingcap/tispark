/*
 *
 * Copyright 2017 PingCAP, Inc.
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

package com.pingcap.spark

import java.sql.{Connection, Date, DriverManager, Timestamp}
import java.util.Properties
import java.util.regex.Pattern
import com.pingcap.spark.Utils._

import scala.collection.mutable.ArrayBuffer

class JDBCWrapper(prop: Properties) {
  private val Sep: String = "|"

  private val createdDBs = ArrayBuffer.empty[String]
  private var currentDatabaseName: String = null

  private val connection: Connection = {
    val jdbcUsername = getOrThrow(prop, "tidbuser")
    val jdbcHostname = getOrThrow(prop, "tidbaddr")
    val jdbcPort = Integer.parseInt(getOrThrow(prop, "tidbport"))
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}?user=${jdbcUsername}"

    DriverManager.getConnection(jdbcUrl, jdbcUsername, "")
  }

  def dumpAllTables(path: String): Unit = {
    if (currentDatabaseName == null) {
      throw new IllegalStateException("need initialization")
    }
    val tables = connection.getMetaData().getTables(currentDatabaseName, null, "%", Array("TABLE"))
    while (tables.next()) {
      val table = tables.getString("TABLE_NAME")
      dumpCreateTable(table, path)
      dumpTableContent(table, path)
    }
  }

  private def dumpCreateTable(table: String, path: String): Unit = {
    val (_, res) = queryTiDB("show create table " + table)
    val content = res(0)(1).toString
    writeFile(content, ddlFileName(path, table))
  }

  private def valToString(value: Any): String = value.toString

  private def valFromString(str: String, tp: String): Any = {
    tp match {
      case "VARCHAR" | "CHAR" | "TEXT" => str
      case "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "DECIMAL" | "NUMERIC" => BigDecimal(str)
      case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INTEGER" | "BIGINT" => str.toLong
      case "DATE" => Date.valueOf(str)
      case "TIME" | "TIMESTAMP" | "DATETIME" => Timestamp.valueOf(str)
    }
  }

  private def rowToString(row: List[Any]): String = row.map(valToString).mkString(Sep)

  private def rowFromString(row: String, types: List[String]): List[Any] = {
    row.split(Pattern.quote(Sep)).zip(types).map {
      case (value, colType) => valFromString(value, colType)
    }.toList
  }

  private def dumpTableContent(table: String, path: String): Unit = {
    val query = s"select * from ${table}"
    val (schema, result) = queryTiDB(query)
    val content = table + "\n" +
                  schema.mkString(Sep) + "\n" +
                  result.map(rowToString).mkString("\n")

    Utils.writeFile(content, dataFileName(path, table))
  }

  def createTable(path: String) = {
    val statement = connection.createStatement()
    statement.executeUpdate(readFile(path).mkString("\n"))
  }

  private def insertRow(row: List[Any], table: String): Unit = {
    val placeholders = List.fill(row.size)("?").mkString(",")
    val stat = s"insert into ${table} values (${placeholders})"
    val ps = connection.prepareStatement(stat)
    row.zipWithIndex.map { case (value, index) => {
        value match {
          case f: Float => ps.setFloat(index + 1, f)
          case d: Double => ps.setDouble(index + 1, d)
          case bd: BigDecimal => ps.setBigDecimal(index + 1, bd.bigDecimal)
          case b: Boolean => ps.setBoolean(index + 1, b)
          case s: Short => ps.setShort(index + 1, s)
          case i: Int => ps.setInt(index + 1, i)
          case l: Long => ps.setLong(index + 1, l)
          case d: Date => ps.setDate(index + 1, d)
          case ts: Timestamp => ps.setTimestamp(index + 1, ts)
        }
      }
    }
    ps.executeUpdate()
  }

  def loadTable(path: String): Unit = {
    val lines = readFile(path)
    val (table, schema, rows) = (lines(0), lines(1).split(Pattern.quote(Sep)).toList, lines.drop(2))
    val rowData: List[List[Any]] = rows.map { rowFromString(_, schema) }
    rowData.map(insertRow(_, table))
  }

  def init(databaseName: String): Unit = {
    if (databaseName != null) {
      if(!databaseExists(databaseName)) {
        createDatabase(databaseName, false)
      }
      connection.setCatalog(databaseName)
      currentDatabaseName = databaseName
    } else {
      val sandbox = "sandbox_" + System.nanoTime()
      createDatabase(sandbox)
      connection.setCatalog(sandbox)
      currentDatabaseName = sandbox
    }
  }

  private def databaseExists(databaseName: String): Boolean = {
    val catalogs = connection.getMetaData().getCatalogs()
    while (catalogs.next()) {
      if (catalogs.getString(1).equalsIgnoreCase(databaseName)) return true
    }
    false
  }

  private def createDatabase(dbName: String, cleanup: Boolean = true): Unit = {
    val statement = connection.createStatement()
    statement.executeUpdate("create database " + dbName)
    if (cleanup) createdDBs.append(dbName)
  }

  private def dropDatabases(): Unit = {
    createdDBs.foreach { dbName =>
      val statement = connection.createStatement()
      statement.executeUpdate("drop database " + dbName)
    }
  }

  def queryTiDB(query: String): (List[String], List[List[Any]]) = {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)
    val rsMetaData = resultSet.getMetaData();
    val retSet = ArrayBuffer.empty[List[Any]]
    val retSchema = ArrayBuffer.empty[String]
    for (i <- 1 to rsMetaData.getColumnCount) {
      retSchema += rsMetaData.getColumnTypeName(i)
    }
    while (resultSet.next()) {
      val row = ArrayBuffer.empty[Any]

      for (i <- 1 to rsMetaData.getColumnCount) {
        row += resultSet.getObject(i)
      }
      retSet += row.toList
    }
    (retSchema.toList, retSet.toList)
  }

  def close(): Unit = {
    dropDatabases
    connection.close
  }
}