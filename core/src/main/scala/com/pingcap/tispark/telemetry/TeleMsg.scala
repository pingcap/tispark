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

import com.pingcap.tikv.util.ConcreteBackOffer
import com.pingcap.tikv.{TiConfiguration, TiSession, TwoPhaseCommitter}
import com.pingcap.tispark.TiConfigConst
import com.pingcap.tispark.utils.SystemInfoUtil
import org.apache.spark.sql.SparkSession
import java.util.UUID

/**
 * Telemetry message.
 */
class TeleMsg(sparkSession: SparkSession) {

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
      val conf = TiConfiguration.createDefault(pdAddr.get)
      getTLSParam(conf)
      val tiSession = TiSession.getInstance(conf)
      val snapShot = tiSession.createSnapshot()
      val value = snapShot.get(TRACK_ID.getBytes("UTF-8"))

      if (value.nonEmpty)
        return new String(value, "UTF-8")

      val uuid = TRACK_ID_PREFIX + UUID.randomUUID().toString
      putKeyValue(TRACK_ID, uuid, conf, tiSession)
      uuid
    } catch {
      case _: Throwable =>
        APP_ID_PREFIX + sparkSession.sparkContext.applicationId
    }
  }

  private def putKeyValue(
      key: String,
      value: String,
      conf: TiConfiguration,
      tiSession: TiSession): Unit = {
    val startTS = tiSession.getTimestamp.getVersion
    try {
      val twoPhaseCommitter = new TwoPhaseCommitter(conf, startTS)
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
        throw e
    }
  }

  private def setTime(): Long = {
    System.currentTimeMillis() / 1000L
  }

  private def generateHardwareInfo(): Map[String, Any] = {
    Map[String, Any](
      "os" -> SystemInfoUtil.getOsFamily,
      "version" -> SystemInfoUtil.getOsVersion,
      "cpu" -> SystemInfoUtil.getCpu,
      "memory" -> SystemInfoUtil.getMemoryInfo,
      "disks" -> SystemInfoUtil.getDisks)
  }

  private def getTLSParam(conf: TiConfiguration): Unit = {
    try {
      val sqlConf = sparkSession.sparkContext.getConf
      val TLSEnable = sqlConf.get(TiConfigConst.TIKV_TLS_ENABLE, "false").toBoolean
      if (TLSEnable) {
        conf.setTlsEnable(true)
        conf.setTrustCertCollectionFile(sqlConf.get(TiConfigConst.TIKV_TRUST_CERT_COLLECTION))
        conf.setKeyCertChainFile(sqlConf.get(TiConfigConst.TIKV_KEY_CERT_CHAIN))
        conf.setKeyFile(sqlConf.get(TiConfigConst.TIKV_KEY_FILE))
      }
    } catch {
      case e: Throwable =>
        throw e
    }
  }
}
