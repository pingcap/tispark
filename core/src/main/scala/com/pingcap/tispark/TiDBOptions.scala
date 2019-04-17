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

package com.pingcap.tispark

import java.util.Locale

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Options for the TiDB data source.
 */
class TiDBOptions(@transient val parameters: CaseInsensitiveMap[String]) extends Serializable {

  import TiDBOptions._

  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(TiDBOptions.mergeWithSparkConf(parameters)))
  }

  private def checkAndGet(name: String): String = {
    require(parameters.isDefinedAt(name), s"Option '$name' is required.")
    parameters(name)
  }

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  val address: String = checkAndGet(TIDB_ADDRESS)
  val port: String = checkAndGet(TIDB_PORT)
  val user: String = checkAndGet(TIDB_USER)
  val password: String = checkAndGet(TIDB_PASSWORD)
  val database: String = checkAndGet(TIDB_DATABASE)
  val table: String = checkAndGet(TIDB_TABLE)

  // ------------------------------------------------------------
  // Optional parameters only for writing
  // ------------------------------------------------------------
  // if to truncate the table from TiDB
  val isTruncate: Boolean = parameters.getOrElse(TIDB_TRUNCATE, "false").toBoolean

  // the create table option , which can be table_options or partition_options.
  // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
  val createTableOptions: String = parameters.getOrElse(TIDB_CREATE_TABLE_OPTIONS, "")

  val createTableColumnTypes: Option[String] = parameters.get(TIDB_CREATE_TABLE_COLUMN_TYPES)

  // ------------------------------------------------------------
  // Calculated parameters
  // ------------------------------------------------------------
  val url: String =
    s"jdbc:mysql://address=(protocol=tcp)(host=$address)(port=$port)/?user=$user&password=$password&useSSL=false&rewriteBatchedStatements=true"

  val dbtable: String = s"$database.$table"
}

object TiDBOptions {
  private val tidbOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    tidbOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  private def mergeWithSparkConf(parameters: Map[String, String]): Map[String, String] = {
    val sparkConf = SparkContext.getOrCreate().getConf
    if (sparkConf.get("spark.sql.extensions", "").equals("org.apache.spark.sql.TiExtensions")) {
      // priority: data source config > spark config
      sparkConf.getAll.toMap ++ parameters
    } else {
      parameters
    }
  }

  val TIDB_ADDRESS: String = newOption("tidb.addr")
  val TIDB_PORT: String = newOption("tidb.port")
  val TIDB_USER: String = newOption("tidb.user")
  val TIDB_PASSWORD: String = newOption("tidb.password")
  val TIDB_DATABASE: String = newOption("database")
  val TIDB_TABLE: String = newOption("table")
  val TIDB_TRUNCATE: String = newOption("truncate")
  val TIDB_CREATE_TABLE_OPTIONS: String = newOption("createTableOptions")
  val TIDB_CREATE_TABLE_COLUMN_TYPES: String = newOption("createTableColumnTypes")
}
