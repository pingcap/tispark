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

import java.sql.{Blob, Connection, Date, DriverManager, Timestamp}
import java.util.Properties
import java.util.regex.Pattern

import com.pingcap.spark.Utils._
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.mutable.ArrayBuffer

class JDBCWrapper(prop: Properties) extends LazyLogging {
  private val KeyTiDBAddress = "tidb.addr"
  private val KeyTiDBPort = "tidb.port"
  private val KeyTiDBUser = "tidb.user"
  private val KeyUseRawSparkMySql = "spark.use_raw_mysql"
  private val KeyMysqlAddress = "mysql.addr"
  private val KeyMysqlUser = "mysql.user"
  private val KeyMysqlPassword = "mysql.password"

  private val Sep: String = "|"

  private val createdDBs = ArrayBuffer.empty[String]
  private var currentDatabaseName: String = _

  private val connection: Connection = {
    val useRawSparkMySql: Boolean = getFlag(prop, KeyUseRawSparkMySql)
    val jdbcUsername = if(useRawSparkMySql) getOrThrow(prop, KeyMysqlUser) else getOrThrow(prop, KeyTiDBUser)
    val jdbcHostname = if(useRawSparkMySql) getOrThrow(prop, KeyMysqlAddress) else getOrThrow(prop, KeyTiDBAddress)
    val jdbcPort = if(useRawSparkMySql) 0 else Integer.parseInt(getOrThrow(prop, KeyTiDBPort))
    val jdbcPassword = if(useRawSparkMySql) getOrThrow(prop, KeyMysqlPassword) else ""
    val jdbcUrl = s"jdbc:mysql://$jdbcHostname" +
      (if (useRawSparkMySql) "" else s":$jdbcPort") +
      s"/?user=$jdbcUsername&password=$jdbcPassword"

    logger.info("jdbcUsername: " + jdbcUsername)
    logger.info("jdbcHostname: " + jdbcHostname)
    logger.info("jdbcPassword: " + jdbcPassword)
    logger.info("jdbcPort: " + jdbcPort)
    logger.info("jdbcUrl: " + jdbcUrl)

    DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
  }

  def dumpAllTables(path: String): Unit = {
    if (currentDatabaseName == null) {
      throw new IllegalStateException("need initialization")
    }
    val tables = connection.getMetaData.getTables(currentDatabaseName, null, "%", scala.Array("TABLE"))
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

  private def valToString(value: Any): String = {
    logger.info(if (value == null) "NULL" else value.getClass.toString)
    Option(value).getOrElse("NULL").toString
  }

  private def valFromString(str: String, tp: String): Any = {
    if (str.equalsIgnoreCase("NULL")) {
      logger.info("value is null")
      null
    } else {
      logger.info("value = " + str)
      tp match {
        case "VARCHAR" | "CHAR" | "TEXT" | "TIME" => str
        case "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "DECIMAL" | "NUMERIC" => BigDecimal(str)
        case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" | "YEAR" => str.toLong
        case "DATE" => Date.valueOf(str)
        case "TIMESTAMP" | "DATETIME" => Timestamp.valueOf(str)
        case "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BLOB" => str.getBytes
        case _ => str
      }
    }
  }

  private def typeCodeFromString(tp: String): Int = {
    tp match {
      case "VARCHAR" | "CHAR" | "TEXT" | "TIME" => 12
      case "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "DECIMAL" | "NUMERIC" => 3
      case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" => 4
      case "DATE" => 91
      case "TIMESTAMP" | "DATETIME" => 93
      case "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BLOB" => 2004
//      case "BINARY" => -2
//      case "VARBINARY" => -3
      case _ => 1111
    }
  }

  private def rowToString(row: List[Any]): String = row.map(valToString).mkString(Sep)

  private def rowFromString(row: String, types: List[String]): List[Any] = {
    logger.info(row)

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
    row.zipWithIndex.foreach { case (value, index) =>
      val pos = index + 1
      logger.info(if (value == null) "NULL" else value.getClass.toString)
      value match {
        case bd: BigDecimal => ps.setBigDecimal(pos, bd.bigDecimal)
        case l: Long => ps.setLong(pos, l)
        case d: Date => ps.setDate(pos, d)
        case s: String => ps.setString(pos, s)
        case ts: Timestamp => ps.setTimestamp(pos, ts)
        case ba: Array[Byte] => ps.setBytes(pos, ba)
        case null => ps.setNull(pos, typeCodeFromString(schema(index)))
        case b: Blob => ps.setBlob(pos, b)
      }
    }
    ps.executeUpdate()
  }

  def loadTable(path: String): Unit = {
    try {
      logger.info("Loading data from : " + path)
      val lines = readFile(path)
      val (table, schema, rows) = (lines.head, lines(1).split(Pattern.quote(Sep)).toList, lines.drop(2))
      val rowData: List[List[Any]] = rows.map {
        rowFromString(_, schema)
      }
      rowData.map(insertRow(_, schema, table))
    } catch {
      case e: Exception => logger.error("Error loading table from path: " + e.getMessage)
    }
  }

  def init(databaseName: String): String = {
    if (databaseName != null) {
      logger.info("fetching " + databaseName)
      if (!databaseExists(databaseName)) {
        createDatabase(databaseName, cleanup = false)
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

  def toOutput(value: Any, colType: String): Any = {
    value match {
      case _: scala.Array[Byte] =>
        var str: String = new String
        for (b <- value.asInstanceOf[scala.Array[Byte]]) {
          str = str.concat(b.toString)
        }
        str
      case _: Date if colType.equalsIgnoreCase("YEAR") =>
        value.toString.split("-")(0)
      case default => default
    }
  }

  def execTiDB(query: String): Boolean = {
    try {
      logger.info("Running query on TiDB: " + query)
      val statement = connection.createStatement()
      statement.execute(query)
    } catch {
      case e: Exception =>
        logger.error("Executing \'" + query + "\' failed: " + e.getMessage)
        false
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
        row += toOutput(resultSet.getObject(i), retSchema(i - 1))
      }
      retSet += row.toList
    }
    (retSchema.toList, retSet.toList)
  }

  def getTableColumnNames(tableName: String): List[String] = {
    val rs = connection.createStatement().executeQuery("select * from " + tableName + " limit 1")
    val metaData = rs.getMetaData
    var resList = ArrayBuffer.empty[String]
    for (i <- 1 to metaData.getColumnCount) {
      resList += metaData.getColumnName(i)
    }
    resList.toList
  }

  def close(): Unit = {
    dropDatabases()
    connection.close()
  }
}