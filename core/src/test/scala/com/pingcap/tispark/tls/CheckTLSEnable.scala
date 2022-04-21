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

package com.pingcap.tispark.tls

import java.util.Properties

object CheckTLSEnable {
  val tidbConfig = "tidb_config.properties"
  val TLSEnable = "spark.tispark.tikv.tls_enable"

  // TLS TEST will open only when spark.tispark.tikv.tls_enable == true
  def isEnableTest(): Boolean = {
    val confStream = Thread
      .currentThread()
      .getContextClassLoader
      .getResourceAsStream(tidbConfig)
    val prop = new Properties()

    if (confStream == null) {
      // TLS close, skip this test
      return false
    } else {
      prop.load(confStream)
    }

    var tlsEnable = prop.getProperty(TLSEnable)
    val jvmProp = System.getProperty(TLSEnable)
    tlsEnable = if (jvmProp != null) jvmProp else tlsEnable

    if (tlsEnable != null && tlsEnable.equals("true")) {
      return true
    } else {
      return false
    }
  }
}
