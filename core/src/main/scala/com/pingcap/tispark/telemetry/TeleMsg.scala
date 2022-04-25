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

import com.pingcap.tispark.utils.SystemInfoUtil

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

/**
 * Telemetry message.
 */
object TeleMsg {

  // telemetry message entry
  var uuid: String = setUUID()
  val time: String = setTime()
  val app: String = "TiSpark"
  val hardware: Map[String, Any] = generateHardwareInfo
  val instance: Map[String, Any] = TiSparkTeleInfo.getTiSparkTeleInfo
  val configuration: Map[String, String] = TiSparkTeleConf.getTiSparkTeleConf

  // judge this message should be sent or not
  private var state = MsgState.UNSENT

  /**
   * Message sending strategy.
   * Now strategy: the message is sent only once.
   *
   * @return True means that msg should send, False means shouldn't.
   */
  def shouldSendMsg : Boolean = {
    if(state == MsgState.UNSENT) true else false
  }

  /**
   * Change message state.
   *
   * @param msgState MsgState.UNSENT or MsgState.SENT
   */
  def changeState(msgState: MsgState.Value): Unit = {
      this.state = msgState
  }

  def setUUID(): String = {
    var uuid = ""
    try {
      uuid = System.getProperty("TISPARK_UUID")
      if (uuid == null) {
        uuid = UUID.randomUUID().toString
        System.setProperty("TISPARK_UUID", uuid)
      }
    } catch {
      case _: Throwable => uuid = ""
    }
    uuid
  }

  def setTime() : String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
  }

  def generateHardwareInfo: Map[String, Any] = {
    Map[String, Any](
      "os" -> SystemInfoUtil.getOsFamily,
      "version" -> SystemInfoUtil.getOsVersion,
      "cpu" -> SystemInfoUtil.getCpu,
      "memory" -> SystemInfoUtil.getMemoryInfo,
      "disks" -> SystemInfoUtil.getDisks
    )
  }
}

object MsgState extends Enumeration {
  val SENT = Value
  val UNSENT = Value
}