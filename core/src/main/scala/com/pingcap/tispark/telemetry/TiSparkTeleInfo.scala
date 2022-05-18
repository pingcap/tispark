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
import org.slf4j.LoggerFactory
import scala.reflect.{ClassTag, classTag}
import scala.util.matching.Regex

/**
 * A part of telemetry message about TiSpark version information.
 */
object TiSparkTeleInfo {

  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val tispark_version: String = getTispark_version
  private val tidb_version: String = getPd_version
  private val spark_version: String = org.apache.spark.SPARK_VERSION
  private val cluster_id: String = getCluster_id

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
      "spark_version" -> this.spark_version,
      "tidb_cluster_id" -> this.cluster_id)
  }

  private def getTispark_version: String = {
    val pattern = new Regex("[0-9]\\.[0-9]\\.[0-9]-SNAPSHOT")
    val version = TiSparkVersion.version
    pattern.findFirstIn(version) match {
      case Some(s) => s
      case None => "unknown"
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
      val conf = SparkSession.active.sessionState.conf.clone()
      val pdAddr = conf.getConfString("spark.tispark.pd.addresses", "")
      if (pdAddr.equals("")) {
        return Option.empty[T]
      }

      val url = "http://" + pdAddr + urlPattern

      val httpClient = new HttpClientUtil
      val resp = httpClient.get(url)

      val mapper = new ObjectMapper
      val entry = mapper.readValue(resp.body, classTag[T].runtimeClass)

      Option(entry.asInstanceOf[T])
    } catch {
      case e: Throwable =>
        logger.info("Failed to get PD version " + e.getMessage)
        Option.empty[T]
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
