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

  private val optParamPrefix = "spark.tispark."

  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(TiDBOptions.mergeWithSparkConf(parameters)))
  }

  private def checkAndGet(name: String): String = {
    require(
      parameters.isDefinedAt(name) || parameters.isDefinedAt(s"$optParamPrefix$name"),
      s"Option '$name' is required."
    )
    if (parameters.isDefinedAt(name)) {
      parameters(name)
    } else {
      parameters(s"$optParamPrefix$name")
    }
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
  val replace: Boolean = parameters.getOrElse(TIDB_REPLACE, "false").toBoolean

  // It is an optimize by the nature of 2pc protocol
  // We leave other txn, gc or read to resolve locks.
  val skipCommitSecondaryKey: Boolean =
    parameters.getOrElse(TIDB_SKIP_COMMIT_SECONDARY_KEY, "false").toBoolean

  val regionSplitNum: Int = parameters.getOrElse(TIDB_REGION_SPLIT_NUM, "0").toInt

  val enableRegionSplit: Boolean =
    parameters.getOrElse(TIDB_ENABLE_REGION_SPLIT, "true").toBoolean

  val lockTTLSeconds: Long = parameters.getOrElse(TIDB_LOCK_TTL_SECONDS, "3600").toLong

  val writeConcurrency: Int = parameters.getOrElse(TIDB_WRITE_CONCURRENCY, "0").toInt

  // ------------------------------------------------------------
  // Calculated parameters
  // ------------------------------------------------------------
  val url: String =
    s"jdbc:mysql://address=(protocol=tcp)(host=$address)(port=$port)/?user=$user&password=$password&useSSL=false&rewriteBatchedStatements=true"

  val tiTableRef: TiTableReference = {
    val dbPrefix = parameters.getOrElse(TiConfigConst.DB_PREFIX, "")
    TiTableReference(dbPrefix + database, table)
  }
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
  val TIDB_REPLACE: String = newOption("replace")
  val TIDB_SKIP_COMMIT_SECONDARY_KEY: String = newOption("skipCommitSecondaryKey")
  val TIDB_REGION_SPLIT_NUM: String = newOption("regionSplitNum")
  val TIDB_ENABLE_REGION_SPLIT: String = newOption("enableRegionSplit")
  val TIDB_LOCK_TTL_SECONDS: String = newOption("lockTTLSeconds")
  val TIDB_WRITE_CONCURRENCY: String = newOption("writeConcurrency")
}
