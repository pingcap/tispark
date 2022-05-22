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

import org.scalatest.FunSuite
import org.scalatest.Matchers.{be, noException}
import org.tikv.common.{TiConfiguration, TiSession}

class TiKVClientTLSSuite extends FunSuite {

  test("test client-java TLS connection") {
    if (!CheckTLSEnable.isEnableTest()) {
      cancel
    }
    noException should be thrownBy {
      val conf = TiConfiguration.createDefault("pd:2379")
      conf.setTlsEnable(true)
      conf.setTrustCertCollectionFile("/config/cert/pem/root.pem")
      conf.setKeyCertChainFile("/config/cert/pem/client.pem")
      conf.setKeyFile("/config/cert/pem/client-pkcs8.key")
      val session = TiSession.getInstance(conf)
      val pdClient = session.getPDClient
      pdClient.updateLeader()
      session.getCatalog()
      session.createSnapshot()
    }
  }

  test("test client-java TLS JKS connection") {
    if (!CheckTLSEnable.isEnableTest()) {
      cancel
    }
    noException should be thrownBy {
      val conf = TiConfiguration.createDefault("pd:2379")
      conf.setTlsEnable(true)
      conf.setJksEnable(true)
      conf.setJksKeyPath("/config/cert/jks/client-keystore")
      conf.setJksKeyPassword("123456")
      conf.setJksTrustPath("/config/cert/jks/server-cert-store")
      conf.setJksTrustPassword("12345678")
      val session = TiSession.getInstance(conf)
      val pdClient = session.getPDClient
      pdClient.updateLeader()
      session.getCatalog()
      session.createSnapshot()
    }
  }
}
