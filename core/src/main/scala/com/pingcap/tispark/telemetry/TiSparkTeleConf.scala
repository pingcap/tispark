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

import com.pingcap.tispark.TiConfigConst
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * A part of telemetry message about TiSpark configuration.
 */
object TiSparkTeleConf {

  private val logger = LoggerFactory.getLogger(getClass.getName)
  private var defaultConfMap: Map[String, String] = Map[String, String]()
  private var tiSparkTeleConf: Map[String, String] = Map[String, String]()

  generateDefaultConfList()
  generateTiSparkTeleConf()

  /**
   * Get the newest TiSparkTeleConf.
   *
   * @return telemetry message about TiSpark configuration
   */
  def getTiSparkTeleConf(): Map[String, String] = {
    generateTiSparkTeleConf()
    tiSparkTeleConf
  }

  /**
   * set TiSparkTeleConf.
   *
   * @param conf  configuration name
   * @param value configuration value
   */
  def setTiSparkTeleConf(conf: String, value: String): Unit = {
    tiSparkTeleConf += (conf -> value)
  }

  private def generateDefaultConfList(): Unit = {
    defaultConfMap += (TiConfigConst.ALLOW_AGG_PUSHDOWN -> "false")
    defaultConfMap += (TiConfigConst.ALLOW_INDEX_READ -> "true")
    defaultConfMap += (TiConfigConst.INDEX_SCAN_BATCH_SIZE -> "20000")
    defaultConfMap += (TiConfigConst.INDEX_SCAN_CONCURRENCY -> "5")
    defaultConfMap += (TiConfigConst.REQUEST_COMMAND_PRIORITY -> "LOW")
    defaultConfMap += (TiConfigConst.REQUEST_ISOLATION_LEVEL -> "SI")
    defaultConfMap += (TiConfigConst.USE_INDEX_SCAN_FIRST -> "false")
    defaultConfMap += (TiConfigConst.COPROCESS_STREAMING -> "false")
    defaultConfMap += (TiConfigConst.CODEC_FORMAT -> "chblock")
    defaultConfMap += (TiConfigConst.UNSUPPORTED_TYPES -> "")
    defaultConfMap += (TiConfigConst.CHUNK_BATCH_SIZE -> "1024")
    defaultConfMap += (TiConfigConst.SHOW_ROWID -> "false")
    defaultConfMap += (TiConfigConst.ISOLATION_READ_ENGINES -> "tikv")
    defaultConfMap += ("spark.sql.auth.enable" -> "false")
  }

  private def generateTiSparkTeleConf(): Unit = {
    try {
      val sparkConf = SparkSession.active.sessionState.conf.clone()
      for ((conf, defaultValue) <- defaultConfMap) {
        tiSparkTeleConf += (conf -> sparkConf.getConfString(conf, defaultValue))
      }
    } catch {
      case e: Throwable => logger.warn("Failed to get tispark configuration of telemetry", e)
    }
  }
}
