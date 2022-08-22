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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

import scala.collection.mutable.ListBuffer

/**
 * Options for the TiDB data source.
 */
class TiDBOptions(@transient val parameters: CaseInsensitiveMap[String]) extends Serializable {

  import com.pingcap.tispark.write.TiDBOptions._

  private val optParamPrefix = "spark.tispark."

  private val requiredWriteOptions = ListBuffer[String]()

  // ------------------------------------------------------------
  // Required parameters for Write
  // ------------------------------------------------------------
  val address: String = getWriteRequiredOrNull(TIDB_ADDRESS)
  val port: String = getWriteRequiredOrNull(TIDB_PORT)
  val user: String = getWriteRequiredOrNull(TIDB_USER)
  val password: String = getWriteRequiredOrNull(TIDB_PASSWORD)
  val multiTables: Boolean = getOrDefault(TIDB_MULTI_TABLES, "false").toBoolean
  val database: String = if (!multiTables) {
    getWriteRequiredOrNull(TIDB_DATABASE)
  } else {
    getOrDefault(TIDB_DATABASE, "")
  }
  val table: String = if (!multiTables) {
    getWriteRequiredOrNull(TIDB_TABLE)
  } else {
    getOrDefault(TIDB_TABLE, "")
  }

  def checkWriteRequired(): Unit = {
    requiredWriteOptions.foreach(
      name =>
        require(
          parameters.isDefinedAt(name) || parameters.isDefinedAt(s"$optParamPrefix$name"),
          s"Option '$name' is required."))
  }
  // ------------------------------------------------------------
  // Optional parameters for writing
  // ------------------------------------------------------------
  val replace: Boolean = getOrDefault(TIDB_REPLACE, "false").toBoolean
  // ttlMode = { "FIXED", "UPDATE", "DEFAULT" }
  val ttlMode: String = getOrDefault(TIDB_TTL_MODE, "DEFAULT").toUpperCase()
  val useSnapshotBatchGet: Boolean = getOrDefault(TIDB_USE_SNAPSHOT_BATCH_GET, "true").toBoolean
  //20k
  val snapshotBatchGetSize: Int = getOrDefault(TIDB_SNAPSHOT_BATCH_GET_SIZE, "20480").toInt
  val batchGetBackOfferMS: Int = getOrDefault(TIDB_BATCH_GET_BACKOFFER_MS, "60000").toInt
  val enableUpdateTableStatistics: Boolean =
    getOrDefault(TIDB_ENABLE_UPDATE_TABLE_STATISTICS, "false").toBoolean
  val deduplicate: Boolean = getOrDefault(TIDB_DEDUPLICATE, "true").toBoolean
  // TiDB use V2 row format to save data after 4.0.0. It can read both V1 and V2 rows.
  // TiSpark only support TiDB which version higher than 4.0.0. And it can read both V1 and V2 rows too.
  // So it better to save data using V2 row format.
  val tidbRowFormatVersion: Int = getOrDefault(TiDB_ROW_FORMAT_VERSION, "2").toInt

  // ------------------------------------------------------------
  // Optional parameters for Test
  // ------------------------------------------------------------
  val sleepBeforePrewritePrimaryKey: Long =
    getOrDefault(TIDB_SLEEP_BEFORE_PREWRITE_PRIMARY_KEY, "0").toLong
  val sleepAfterPrewritePrimaryKey: Long =
    getOrDefault(TIDB_SLEEP_AFTER_PREWRITE_PRIMARY_KEY, "0").toLong
  val sleepAfterPrewriteSecondaryKey: Long =
    getOrDefault(TIDB_SLEEP_AFTER_PREWRITE_SECONDARY_KEY, "0").toLong
  val sleepAfterGetCommitTS: Long = getOrDefault(TIDB_SLEEP_AFTER_GET_COMMIT_TS, "0").toLong
  val isTest: Boolean = getOrDefault(TIDB_IS_TEST, "false").toBoolean

  // ------------------------------------------------------------
  // Optional parameters for 2PC
  // ------------------------------------------------------------
  val prewriteBackOfferMS: Int = getOrDefault(TIDB_PREWRITE_BACKOFFER_MS, "240000").toInt
  val commitBackOfferMS: Int = getOrDefault(TIDB_COMMIT_BACKOFFER_MS, "20000").toInt
  //https://github.com/pingcap/tispark/pull/1599
  // 16 * 1024 = 16K
  val txnPrewriteBatchSize: Long = getOrDefault(TIDB_TXN_PREWITE_BATCH_SIZE, "16384").toLong
  // 16 * 1024 = 16K
  val txnCommitBatchSize: Long = getOrDefault(TIDB_TXN_COMMIT_BATCH_SIZE, "16384").toLong
  // 32 * 1024
  val writeBufferSize: Int = getOrDefault(TIDB_WRITE_BUFFER_SIZE, "32768").toInt
  val writeThreadPerTask: Int = getOrDefault(TIDB_WRITE_THREAD_PER_TASK, "2").toInt
  val retryCommitSecondaryKey: Boolean =
    getOrDefault(TIDB_RETRY_COMMIT_SECONDARY_KEY, "true").toBoolean
  val prewriteMaxRetryTimes: Int = getOrDefault(TIDB_PREWRITE_MAX_RETRY_TIMES, "64").toInt
  val commitPrimaryKeyRetryNumber: Int =
    getOrDefault(TIDB_COMMIT_PRIMARY_KEY_RETRY_NUMBER, "4").toInt
  // It is an optimize by the nature of 2pc protocol
  // We leave other txn, gc or read to resolve locks.
  val skipCommitSecondaryKey: Boolean =
    getOrDefault(TIDB_SKIP_COMMIT_SECONDARY_KEY, "false").toBoolean

  // ------------------------------------------------------------
  // Optional parameters for region split
  // ------------------------------------------------------------
  val enableRegionSplit: Boolean = getOrDefault(TIDB_ENABLE_REGION_SPLIT, "true").toBoolean
  val regionSplitNum: Int = getOrDefault(TIDB_REGION_SPLIT_NUM, "0").toInt
  val sampleSplitFrac: Int = getOrDefault(TIDB_SAMPLE_SPLIT_FRAC, "1000").toInt
  val scatterWaitMS: Int = getOrDefault(TIDB_SCATTER_WAIT_MS, "300000").toInt
  val regionSplitKeys: Int = getOrDefault(TIDB_REGION_SPLIT_KEYS, "960000").toInt
  val minRegionSplitNum: Int = getOrDefault(TIDB_MIN_REGION_SPLIT_NUM, "8").toInt
  val maxRegionSplitNum: Int = getOrDefault(TIDB_MAX_REGION_SPLIT_NUM, "4096").toInt
  val regionSplitThreshold: Int = getOrDefault(TIDB_REGION_SPLIT_THRESHOLD, "100000").toInt
  val splitRegionBackoffMS: Int = getOrDefault(TIDB_SPLIT_REGION_BACKOFFER_MS, "120000").toInt
  val scatterRegionBackoffMS: Int = getOrDefault(TIDB_SCATTER_REGION_BACKOFFER_MS, "30000").toInt
  val regionSplitUsingSize: Boolean =
    getOrDefault(TIDB_REGION_SPLIT_USING_SIZE, "true").toBoolean
  //96M
  val bytesPerRegion: Int = getOrDefault(TIDB_BYTES_PER_REGION, "100663296").toInt
  val maxWriteTaskNumber: Int = getOrDefault(TIDB_MAX_WRITE_TASK_NUMBER, "0").toInt

  // ------------------------------------------------------------
  // Optional parameters for read
  // ------------------------------------------------------------
  val tidbRowId: Boolean = getOrDefault(TIDB_ROWID, "false").toBoolean

  // ------------------------------------------------------------
  // Enable JDBC SSL connection
  // ------------------------------------------------------------
  var SSLParameters: String = getOrDefault(TIDB_ENABLE_JDBC_SSL, "false")
  if (SSLParameters.equals("true")) {
    val FILE_PREFIX = "file:"
    var clientCertStore = getOrDefault(TIDB_JDBC_CLIENT_CERT_STORE, "")
    val clientCertPassword = getOrDefault(TIDB_JDBC_CLIENT_CERT_PASSWORD, "")
    var serverCertStore = getOrDefault(TIDB_JDBC_SERVER_CERT_STORE, "")
    val serverCertPassword = getOrDefault(TIDB_JDBC_SERVER_CERT_PASSWORD, "")
    // Set up Server authentication
    if (serverCertStore.equals("")) {
      SSLParameters = "true&requireSSL=true&verifyServerCertificate=false"
    } else {
      if (!serverCertStore.startsWith(FILE_PREFIX))
        serverCertStore = FILE_PREFIX + serverCertStore
      SSLParameters =
        "true&requireSSL=true&verifyServerCertificate=true&trustCertificateKeyStoreUrl=" +
          serverCertStore + "&trustCertificateKeyStorePassword=" + serverCertPassword
    }
    // Setting up client authentication
    if (!clientCertStore.equals("")) {
      if (!clientCertStore.startsWith(FILE_PREFIX))
        clientCertStore = FILE_PREFIX + clientCertStore
      SSLParameters += "&clientCertificateKeyStoreUrl=" + clientCertStore +
        "&clientCertificateKeyStorePassword=" + clientCertPassword
    }
  }

  // ------------------------------------------------------------
  // Calculated parameters
  // ------------------------------------------------------------
  val url: String =
    s"jdbc:mysql://address=(protocol=tcp)(host=$address)(port=$port)/?user=$user&password=$password&useSSL=$SSLParameters&rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.2,TLSv1.3"
      .replaceAll("%", "%25")

  def useTableLock(isV4: Boolean): Boolean = {
    if (isV4) {
      getOrDefault(TIDB_USE_TABLE_LOCK, "false").toBoolean
    } else {
      getOrDefault(TIDB_USE_TABLE_LOCK, "true").toBoolean
    }
  }

  def getTiTableRef(conf: TiConfiguration): TiTableReference =
    TiTableReference(conf.getDBPrefix + database, table)

  def getLockTTLSeconds(tikvSupportUpdateTTL: Boolean): Long = {
    parameters.get(TIDB_LOCK_TTL_SECONDS) match {
      case Some(v) => v.toLong
      case None =>
        if (isTTLUpdate(tikvSupportUpdateTTL)) {
          TTLManager.MANAGED_LOCK_TTL / 1000
        } else {
          3600L
        }
    }
  }

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

  def setDBTable(dBTable: DBTable): TiDBOptions = {
    new TiDBOptions(
      parameters ++ Map(
        TIDB_DATABASE -> dBTable.database,
        TIDB_TABLE -> dBTable.table,
        TIDB_MULTI_TABLES -> "false"))
  }

  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(TiDBOptions.mergeWithSparkConf(parameters)))
  }

  def this() = {
    this(Map[String, String]())
  }

  private def checkAndGet(name: String): String = {
    require(
      parameters.isDefinedAt(name) || parameters.isDefinedAt(s"$optParamPrefix$name"),
      s"Option '$name' is required.")
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

  private def getWriteRequiredOrNull(name: String): String = {
    requiredWriteOptions += name
    if (parameters.isDefinedAt(name)) {
      parameters(name)
    } else if (parameters.isDefinedAt(s"$optParamPrefix$name")) {
      parameters(s"$optParamPrefix$name")
    } else {
      null
    }
  }
}

object TiDBOptions {
  val TIDB_ADDRESS: String = newOption("tidb.addr")
  val TIDB_PORT: String = newOption("tidb.port")
  val TIDB_USER: String = newOption("tidb.user")
  val TIDB_PASSWORD: String = newOption("tidb.password")
  val TIDB_DATABASE: String = newOption("database")
  val TIDB_TABLE: String = newOption("table")
  val TIDB_REPLACE: String = newOption("replace")
  val TIDB_SKIP_COMMIT_SECONDARY_KEY: String = newOption("skipCommitSecondaryKey")
  val TIDB_TTL_MODE: String = newOption("ttlMode")
  val TIDB_USE_SNAPSHOT_BATCH_GET: String = newOption("useSnapshotBatchGet")
  val TIDB_SNAPSHOT_BATCH_GET_SIZE: String = newOption("snapshotBatchGetSize")
  val TIDB_BATCH_GET_BACKOFFER_MS: String = newOption("batchGetBackOfferMS")
  val TIDB_USE_TABLE_LOCK: String = newOption("useTableLock")
  val TIDB_MULTI_TABLES: String = newOption("multiTables")
  val TIDB_PREWRITE_BACKOFFER_MS: String = newOption("prewriteBackOfferMS")
  val TIDB_COMMIT_BACKOFFER_MS: String = newOption("commitBackOfferMS")
  val TIDB_TXN_PREWITE_BATCH_SIZE: String = newOption("txnPrewriteBatchSize")
  val TIDB_TXN_COMMIT_BATCH_SIZE: String = newOption("txnCommitBatchSize")
  val TIDB_WRITE_BUFFER_SIZE: String = newOption("writeBufferSize")
  val TIDB_WRITE_THREAD_PER_TASK: String = newOption("writeThreadPerTask")
  val TIDB_RETRY_COMMIT_SECONDARY_KEY: String = newOption("retryCommitSecondaryKey")
  val TIDB_PREWRITE_MAX_RETRY_TIMES: String = newOption("prewriteMaxRetryTimes")
  val TIDB_COMMIT_PRIMARY_KEY_RETRY_NUMBER: String = newOption("commitPrimaryKeyRetryNumber")
  val TIDB_ENABLE_UPDATE_TABLE_STATISTICS: String = newOption("enableUpdateTableStatistics")
  val TIDB_DEDUPLICATE: String = newOption("deduplicate")
  val TIDB_ROWID: String = newOption("tidbRowId")
  val TiDB_ROW_FORMAT_VERSION: String = newOption("rowFormatVersion")

  // region split
  val TIDB_ENABLE_REGION_SPLIT: String = newOption("enableRegionSplit")
  val TIDB_REGION_SPLIT_NUM: String = newOption("regionSplitNum")
  val TIDB_SAMPLE_SPLIT_FRAC: String = newOption("sampleSplitFrac")
  val TIDB_SCATTER_WAIT_MS: String = newOption("scatterWaitMS")
  val TIDB_REGION_SPLIT_KEYS: String = newOption("regionSplitKeys")
  val TIDB_MIN_REGION_SPLIT_NUM: String = newOption("minRegionSplitNum")
  val TIDB_MAX_REGION_SPLIT_NUM: String = newOption("maxRegionSplitNum")
  val TIDB_REGION_SPLIT_THRESHOLD: String = newOption("regionSplitThreshold")
  val TIDB_SPLIT_REGION_BACKOFFER_MS: String = newOption("splitRegionBackoffMS")
  val TIDB_SCATTER_REGION_BACKOFFER_MS: String = newOption("scatterRegionBackoffMS")
  val TIDB_REGION_SPLIT_USING_SIZE: String = newOption("regionSplitUsingSize")
  val TIDB_BYTES_PER_REGION: String = newOption("bytesPerRegion")
  val TIDB_MAX_WRITE_TASK_NUMBER: String = newOption("maxWriteTaskNumber")

  // ------------------------------------------------------------
  // parameters only for test
  // ------------------------------------------------------------
  val TIDB_IS_TEST: String = newOption("isTest")
  val TIDB_LOCK_TTL_SECONDS: String = newOption("lockTTLSeconds")
  val TIDB_SLEEP_BEFORE_PREWRITE_PRIMARY_KEY: String = newOption("sleepBeforePrewritePrimaryKey")
  val TIDB_SLEEP_AFTER_PREWRITE_PRIMARY_KEY: String = newOption("sleepAfterPrewritePrimaryKey")
  val TIDB_SLEEP_AFTER_PREWRITE_SECONDARY_KEY: String = newOption(
    "sleepAfterPrewriteSecondaryKey")
  val TIDB_SLEEP_AFTER_GET_COMMIT_TS: String = newOption("sleepAfterGetCommitTS")

  // TLS
  val TIDB_ENABLE_JDBC_SSL: String = newOption("jdbc.tls_enable")
  val TIDB_JDBC_CLIENT_CERT_STORE: String = newOption("jdbc.client_cert_store")
  val TIDB_JDBC_CLIENT_CERT_PASSWORD: String = newOption("jdbc.client_cert_password")
  val TIDB_JDBC_SERVER_CERT_STORE: String = newOption("jdbc.server_cert_store")
  val TIDB_JDBC_SERVER_CERT_PASSWORD: String = newOption("jdbc.server_cert_password")

  private def newOption(name: String): String = {
    name.toLowerCase(Locale.ROOT)
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
            "!Security! Please DO NOT add TiDB password to SparkConf which will be shown on Spark WebUI!")
        }
    }
  }
}
