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

import com.pingcap.tikv.{ClientSession, TiConfiguration}
import com.pingcap.tispark.utils.{SystemInfoUtil, TiUtil}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.tikv.common.TiSession
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.txn.TwoPhaseCommitter

import java.util.UUID

/**
 * Telemetry message.
 */
class TeleMsg(sparkSession: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass.getName)
  private final val TRACK_ID = "TiSparkTelemetryId"
  private final val pdAddr: Option[String] = TiSparkTeleInfo.pdAddress
  private final val APP_ID_PREFIX = "appid_"
  private final val TRACK_ID_PREFIX = "trkid_"

  // telemetry message entry
  var track_id: String = generateTrackId()
  val time: Long = setTime()
  val hardware: Map[String, Any] = generateHardwareInfo()
  val instance: Map[String, Any] = TiSparkTeleInfo.getTiSparkTeleInfo()
  val configuration: Map[String, Any] = TiSparkTeleConf.getTiSparkTeleConf()

  private def generateTrackId(): String = {
    try {

      val clientSession =
        ClientSession.getInstance(TiConfiguration.createDefault(pdAddr.get))
      val conf = clientSession.getConf
      TiUtil.sparkConfToTiConfWithoutPD(SparkSession.active.sparkContext.getConf, conf)
      val tiSession = clientSession.getTikvSession
      val snapShot = clientSession.createSnapshot()
      val value = snapShot.get(TRACK_ID.getBytes("UTF-8"))

      if (value.nonEmpty)
        return new String(value, "UTF-8")

      val uuid = TRACK_ID_PREFIX + UUID.randomUUID().toString
      putKeyValue(TRACK_ID, uuid, tiSession)
      uuid
    } catch {
      case e: Throwable =>
        logger.warn("Failed to generated telemetry track ID", e.getMessage)
        APP_ID_PREFIX + sparkSession.sparkContext.applicationId
    }
  }

  private def putKeyValue(key: String, value: String, tiSession: TiSession): Unit = {
    val startTS = tiSession.getTimestamp.getVersion
    try {
      val twoPhaseCommitter = new TwoPhaseCommitter(tiSession, startTS)
      val backOffer = ConcreteBackOffer.newCustomBackOff(1000)
      twoPhaseCommitter.prewritePrimaryKey(
        backOffer,
        key.getBytes("UTF-8"),
        value.getBytes("UTF-8"))
      twoPhaseCommitter.commitPrimaryKey(
        backOffer,
        key.getBytes("UTF-8"),
        tiSession.getTimestamp.getVersion)
    } catch {
      case e: Throwable =>
        logger.warn("Failed to set telemetry ID to TiKV.", e.getMessage)
        throw e
    }
  }

  private def setTime(): Long = {
    System.currentTimeMillis() / 1000L
  }

  private def generateHardwareInfo(): Map[String, Any] = {
    try {
      val hardwareInfo = Map[String, Any](
        "os" -> SystemInfoUtil.getOsFamily,
        "version" -> SystemInfoUtil.getOsVersion,
        "cpu" -> SystemInfoUtil.getCpu,
        "memory" -> SystemInfoUtil.getMemoryInfo,
        "disks" -> SystemInfoUtil.getDisks)
      hardwareInfo
    } catch {
      case e: Throwable =>
        logger.warn("Failed to get hardware information.", e.getMessage)
        null
    }
  }
}
