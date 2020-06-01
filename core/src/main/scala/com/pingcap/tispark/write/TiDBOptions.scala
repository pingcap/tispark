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

package com.pingcap.tispark.write

import java.util.Locale

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.{TTLManager, TiConfiguration}
import com.pingcap.tispark.TiTableReference
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

  private def getOrDefault(name: String, default: String): String = {
    if (parameters.isDefinedAt(name)) {
      parameters(name)
    } else if (parameters.isDefinedAt(s"$optParamPrefix$name")) {
      parameters(s"$optParamPrefix$name")
    } else {
      default
    }
  }

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  val address: String = checkAndGet(TIDB_ADDRESS)
  val port: String = checkAndGet(TIDB_PORT)
  val user: String = checkAndGet(TIDB_USER)
  val password: String = checkAndGet(TIDB_PASSWORD)
  val multiTables: Boolean = parameters.getOrElse(TIDB_MULTI_TABLES, "false").toBoolean
  val database: String = if (!multiTables) {
    checkAndGet(TIDB_DATABASE)
  } else {
    getOrDefault(TIDB_DATABASE, "")
  }
  val table: String = if (!multiTables) {
    checkAndGet(TIDB_TABLE)
  } else {
    getOrDefault(TIDB_TABLE, "")
  }

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
    parameters.getOrElse(TIDB_ENABLE_REGION_SPLIT, "false").toBoolean

  val writeConcurrency: Int = parameters.getOrElse(TIDB_WRITE_CONCURRENCY, "0").toInt

  // ttlMode = { "FIXED", "UPDATE", "DEFAULT" }
  val ttlMode: String = parameters.getOrElse(TIDB_TTL_MODE, "DEFAULT").toUpperCase()

  val useSnapshotBatchGet: Boolean =
    parameters.getOrElse(TIDB_USE_SNAPSHOT_BATCH_GET, "true").toBoolean

  val snapshotBatchGetSize: Int = parameters.getOrElse(TIDB_SNAPSHOT_BATCH_GET_SIZE, "2048").toInt

  val useTableLock: Boolean = parameters.getOrElse(TIDB_USE_TABLE_LOCK, "true").toBoolean

  val sleepAfterPrewritePrimaryKey: Long =
    parameters.getOrElse(TIDB_SLEEP_AFTER_PREWRITE_PRIMARY_KEY, "0").toLong

  val sleepAfterPrewriteSecondaryKey: Long =
    parameters.getOrElse(TIDB_SLEEP_AFTER_PREWRITE_SECONDARY_KEY, "0").toLong

  val sleepAfterGetCommitTS: Long = parameters.getOrElse(TIDB_SLEEP_AFTER_GET_COMMIT_TS, "0").toLong

  val isTest: Boolean = parameters.getOrElse(TIDB_IS_TEST, "false").toBoolean

  // ------------------------------------------------------------
  // Calculated parameters
  // ------------------------------------------------------------
  val url: String =
    s"jdbc:mysql://address=(protocol=tcp)(host=$address)(port=$port)/?user=$user&password=$password&useSSL=false&rewriteBatchedStatements=true"

  def getTiTableRef(conf: TiConfiguration): TiTableReference =
    TiTableReference(conf.getDBPrefix + database, table)

  def isTTLUpdate(tikvSupportUpdateTTL: Boolean): Boolean = {
    if (tikvSupportUpdateTTL) {
      !ttlMode.equals("FIXED")
    } else {
      if (ttlMode.equals("UPDATE")) {
        throw new TiBatchWriteException("current tikv does not support ttl update!")
      }
      false
    }
  }

  def getLockTTLSeconds(tikvSupportUpdateTTL: Boolean): Long =
    if (isTTLUpdate(tikvSupportUpdateTTL)) {
      TTLManager.MANAGED_LOCK_TTL / 1000
    } else {
      parameters.getOrElse(TIDB_LOCK_TTL_SECONDS, "3600").toLong
    }

  def setDBTable(dBTable: DBTable): TiDBOptions = {
    new TiDBOptions(
      parameters ++ Map(
        TIDB_DATABASE -> dBTable.database,
        TIDB_TABLE -> dBTable.table,
        TIDB_MULTI_TABLES -> "false"
      )
    )
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
      val confMap = sparkConf.getAll.toMap
      checkTiDBPassword(confMap)
      confMap ++ parameters
    } else {
      parameters
    }
  }

  private def checkTiDBPassword(conf: Map[String, String]): Unit = {
    conf.foreach {
      case (k, _) =>
        if ("tidb.password".equals(k) || "spark.tispark.tidb.password".equals(k)) {
          throw new TiBatchWriteException(
            "!Security! Please DO NOT add TiDB password to SparkConf which will be shown on Spark WebUI!"
          )
        }
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
  val TIDB_ENABLE_REGION_SPLIT: String = newOption("enableRegionSplit")
  val TIDB_REGION_SPLIT_NUM: String = newOption("regionSplitNum")
  val TIDB_WRITE_CONCURRENCY: String = newOption("writeConcurrency")
  val TIDB_TTL_MODE: String = newOption("ttlMode")
  val TIDB_USE_SNAPSHOT_BATCH_GET: String = newOption("useSnapshotBatchGet")
  val TIDB_SNAPSHOT_BATCH_GET_SIZE: String = newOption("snapshotBatchGetSize")
  val TIDB_USE_TABLE_LOCK: String = newOption("useTableLock")
  val TIDB_MULTI_TABLES: String = newOption("multiTables")

  // ------------------------------------------------------------
  // parameters only for test
  // ------------------------------------------------------------
  val TIDB_IS_TEST: String = newOption("isTest")
  val TIDB_LOCK_TTL_SECONDS: String = newOption("lockTTLSeconds")
  val TIDB_SLEEP_AFTER_PREWRITE_PRIMARY_KEY: String = newOption("sleepAfterPrewritePrimaryKey")
  val TIDB_SLEEP_AFTER_PREWRITE_SECONDARY_KEY: String = newOption("sleepAfterPrewriteSecondaryKey")
  val TIDB_SLEEP_AFTER_GET_COMMIT_TS: String = newOption("sleepAfterGetCommitTS")
}
