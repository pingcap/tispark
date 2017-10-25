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
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.mutable.ArrayBuffer

class JDBCWrapper(prop: Properties) extends LazyLogging {
  private val KeyTiDBAddress = "tidb.addr"
  private val KeyTiDBPort = "tidb.port"
  private val KeyTiDBUser = "tidb.user"

  private val Sep: String = "|"

  private val createdDBs = ArrayBuffer.empty[String]
  private var currentDatabaseName: String = _

  private val connection: Connection = {
    val jdbcUsername = getOrThrow(prop, KeyTiDBUser)
    val jdbcHostname = getOrThrow(prop, KeyTiDBAddress)
    val jdbcPort = Integer.parseInt(getOrThrow(prop, KeyTiDBPort))
    val jdbcUrl = s"jdbc:mysql://$jdbcHostname:$jdbcPort?user=$jdbcUsername"

    logger.info("jdbcUsername: " + jdbcUsername)
    logger.info("jdbcHostname: " + jdbcHostname)
    logger.info("jdbcPort: " + jdbcPort)
    logger.info("jdbcUrl: " + jdbcUrl)

    DriverManager.getConnection(jdbcUrl, jdbcUsername, "")
  }

  def dumpAllTables(path: String): Unit = {
    if (currentDatabaseName == null) {
      throw new IllegalStateException("need initialization")
    }
    val tables = connection.getMetaData.getTables(currentDatabaseName, null, "%", Array("TABLE"))
    while (tables.next()) {
      val table = tables.getString("TABLE_NAME")
      dumpCreateTable(table, path)
      dumpTableContent(table, path)
    }
  }

  private def dumpCreateTable(table: String, path: String): Unit = {
    logger.info(s"Dumping table: $table to $path")
    val (_, res) = queryTiDB("show create table " + table)
    var content = "DROP TABLE IF EXISTS "
    content = content.concat("`" + table + "`;\n")
    content = content.concat(res.head(1).toString)
    writeFile(content, ddlFileName(path, table))
  }

  private def valToString(value: Any): String = Option(value).getOrElse("NULL").toString

  private def valFromString(str: String, tp: String): Any = {
    if (str.equalsIgnoreCase("NULL")) {
      null
    } else {
      tp match {
        case "VARCHAR" | "CHAR" | "TEXT" => str
        case "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "DECIMAL" | "NUMERIC" => BigDecimal(str)
        case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" => str.toLong
        case "DATE" => Date.valueOf(str)
        case "TIME" | "TIMESTAMP" | "DATETIME" => Timestamp.valueOf(str)
      }
    }
  }

  private def typeCodeFromString(tp: String): Int = {
    tp match {
      case "VARCHAR" | "CHAR" | "TEXT" => 12
      case "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "DECIMAL" | "NUMERIC" => 3
      case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" => 4
      case "DATE" => 91
      case "TIME" | "TIMESTAMP" | "DATETIME" => 93
    }
  }

  private def rowToString(row: List[Any]): String = row.map(valToString).mkString(Sep)

  private def rowFromString(row: String, types: List[String]): List[Any] = {
    row.split(Pattern.quote(Sep)).zip(types).map {
      case (value, colType) => valFromString(value, colType)
    }.toList
  }

  private def dumpTableContent(table: String, path: String): Unit = {
    val query = s"select * from $table"
    val (schema, result) = queryTiDB(query)
    val content = table + "\n" +
                  schema.mkString(Sep) + "\n" +
                  result.map(rowToString).mkString("\n")

    writeFile(content, dataFileName(path, table))
  }

  def createTable(path: String): Unit = {
    val statement = connection.createStatement()
    val query = readFile(path).mkString("\n")
    logger.info("Running script: " + path)
    logger.info("Create table: " + query)
    statement.executeUpdate(query)
  }

  private def insertRow(row: List[Any], schema: List[String], table: String) = {
    logger.info("Insert into : " + table)
    val placeholders = List.fill(row.size)("?").mkString(",")
    val stat = s"insert into $table values ($placeholders)"
    val ps = connection.prepareStatement(stat)
    row.zipWithIndex.map { case (value, index) =>
        val pos = index + 1
        value match {
          case bd: BigDecimal => ps.setBigDecimal(pos, bd.bigDecimal).→(index)
          case l: Long => ps.setLong(pos, l).→(index)
          case d: Date => ps.setDate(pos, d).→(index)
          case s: String => ps.setString(pos, s).→(index)
          case ts: Timestamp => ps.setTimestamp(pos, ts).→(index)
          case null => ps.setNull(pos, typeCodeFromString(schema(index))).→(index)
        }
    }
    ps.executeUpdate()
  }

  def loadTable(path: String): Unit = {
    logger.info("Loading data from : " + path)
    val lines = readFile(path)
    val (table, schema, rows) = (lines.head, lines(1).split(Pattern.quote(Sep)).toList, lines.drop(2))
    val rowData: List[List[Any]] = rows.map { rowFromString(_, schema) }
    rowData.map(insertRow(_, schema, table))
  }

  def init(databaseName: String): String = {
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
    logger.info("Current database " + currentDatabaseName)
    currentDatabaseName
  }

  private def databaseExists(databaseName: String): Boolean = {
    val catalogs = connection.getMetaData.getCatalogs
    while (catalogs.next()) {
      if (catalogs.getString(1).equalsIgnoreCase(databaseName)) return true
    }
    false
  }

  private def createDatabase(dbName: String, cleanup: Boolean = true): Unit = {
    logger.info("Creating database " + dbName)
    val statement = connection.createStatement()
    statement.executeUpdate("create database " + dbName)
    if (cleanup) createdDBs.append(dbName)
  }

  private def dropDatabases(): Unit = {
    createdDBs.foreach { dbName =>
      logger.info("Dropping database " + dbName)
      val statement = connection.createStatement()
      statement.executeUpdate("drop database " + dbName)
    }
  }

  def queryTiDB(query: String): (List[String], List[List[Any]]) = {
    logger.info("Running query on TiDB: " + query)
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)
    val rsMetaData = resultSet.getMetaData
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
    dropDatabases()
    connection.close()
  }
}