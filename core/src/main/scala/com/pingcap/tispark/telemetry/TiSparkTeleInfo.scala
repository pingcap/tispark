/*
 * Copyright 2022 PingCAP, Inc.
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

package com.pingcap.tispark.telemetry

import com.pingcap.tispark.utils.HttpClientUtil
import com.pingcap.tispark.TiSparkVersion
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.databind.ObjectMapper
import com.pingcap.tikv.TiConfiguration
import com.pingcap.tispark.auth.TiAuthorization
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory
import scalaj.http.HttpResponse
import scala.reflect.{ClassTag, classTag}
import scala.util.matching.Regex

/**
 * A part of telemetry message about TiSpark version information.
 */
object TiSparkTeleInfo {

  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val pd_address: Option[String] = getPDAddress
  private val tispark_version: String = getTispark_version
  private val tidb_version: String = getPd_version
  private val spark_version: String = org.apache.spark.SPARK_VERSION

  val tiSparkTeleInfo: Map[String, Any] = generateTiSparkTeleInfo()

  /**
   * Get tiSparkTeleInfo that cantains TiSpark version, TiDB version, Spark Version.
   *
   * @return tiSparkTeleInfo
   */
  def getTiSparkTeleInfo(): Map[String, Any] = tiSparkTeleInfo

  private def generateTiSparkTeleInfo(): Map[String, Any] = {
    Map(
      "tispark_version" -> this.tispark_version,
      "tidb_version" -> this.tidb_version,
      "spark_version" -> this.spark_version)
  }

  def pdAddress: Option[String] = pd_address

  private def getTispark_version: String = {
    val pattern = new Regex("[0-9]\\.[0-9]\\.[0-9]-SNAPSHOT")
    val version = TiSparkVersion.version
    pattern.findFirstIn(version) match {
      case Some(s) => s
      case None => "UNKNOWN"
    }
  }

  private def getPd_version: String = {
    val pDStatusOption = requestPD[PDStatus]("/pd/api/v1/status")
    if (!pDStatusOption.isDefined)
      "UNKNOWN"
    else
      pDStatusOption.get.version
  }

  private def getCluster_id: String = {
    val clusterOption = requestPD[Cluster]("/pd/api/v1/cluster")
    if (!clusterOption.isDefined)
      "UNKNOWN"
    else
      clusterOption.get.id
  }

  private def requestPD[T: ClassTag](urlPattern: String): Option[T] = {
    try {
      if (!pd_address.isDefined) {
        return Option.empty[T]
      }
      val httpClient = new HttpClientUtil
      var resp: HttpResponse[String] = null

      val conf: TiConfiguration = new TiConfiguration
      HttpClientUtil.getTLSParam(conf)

      if (conf.isTlsEnable) {
        val url = "https://" + pd_address.get + urlPattern
        resp = httpClient.getHttps(url, conf)
      } else {
        val url = "http://" + pd_address.get + urlPattern
        resp = httpClient.get(url)
      }

      val mapper = new ObjectMapper
      val entry = mapper.readValue(resp.body, classTag[T].runtimeClass)

      Option(entry.asInstanceOf[T])
    } catch {
      case e: Throwable =>
        logger.info("Failed to get PD version " + e.getMessage)
        Option.empty[T]
    }
  }

  private def getPDAddress: Option[String] = {
    try {
      if (TiAuthorization.enableAuth) {
        Option(TiAuthorization.tiAuthorization.get.getPDAddress())
      } else {
        val conf: SQLConf = SparkSession.active.sessionState.conf.clone()
        Option(conf.getConfString("spark.tispark.pd.addresses"))
      }
    } catch {
      case e: Throwable =>
        logger.info("Failed to get PD Address" + e.getMessage)
        Option.empty
    }
  }
}

/**
 * PD status data class type for HTTP request.
 * Get request return a JSON string that is explained by this class.
 */
class PDStatus {
  var build_ts: String = _
  var version: String = _
  var git_hash: String = _
  var start_timestamp: String = _

  def setBuild_ts(build_ts: String): Unit = this.build_ts = build_ts
  def setVersion(version: String): Unit = this.version = version
  def setGit_hash(git_hash: String): Unit = this.git_hash = git_hash
  def setStart_timestamp(start_timestamp: String): Unit = this.start_timestamp = start_timestamp
}

/**
 * Cluster status data class type for HTTP request.
 * Get request return a JSON that is explained by this class.
 */
class Cluster {
  var id: String = _
  var max_peer_count: Int = _

  def setId(id: String): Unit = this.id = id
  def setMax_peer_count(max_peer_count: Int): Unit = this.max_peer_count = max_peer_count
}
