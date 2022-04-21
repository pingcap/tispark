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
