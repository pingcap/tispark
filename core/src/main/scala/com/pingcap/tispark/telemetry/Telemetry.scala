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

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.pingcap.tispark.utils.HttpClientUtil
import org.slf4j.LoggerFactory

/**
 * Report telemetry by HTTP POST. The url should be constant
 */
class Telemetry {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  // TODO need a real url
  private var url = "https://127.0.0.1:2379"

  /**
   * Send telemetry message.
   *
   * @param msg the msg sent to telemetry server
   */
  def report(msg: TeleMsg): Unit = {
    // Don't try again even if the message failed to be sent
    msg.changeState(MsgState.SENT)
    val httpClient = new HttpClientUtil

    val mapper = new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val msgString = mapper.writeValueAsString(msg)
    logger.info("Telemetry report: " + msgString)

    try {
      httpClient.postJSON(url, msg)
    } catch {
      case e: Throwable => logger.warn("Failed to report telemetry", e)
    }
  }

  def setUrl(url: String): Unit = {
    this.url = url
  }
}
