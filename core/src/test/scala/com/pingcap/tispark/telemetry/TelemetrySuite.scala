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
import com.sun.net.httpserver.{
  HttpExchange,
  HttpHandler,
  HttpServer,
  HttpsConfigurator,
  HttpsServer
}
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.Matchers.{be, noException}

import java.net.InetSocketAddress

class TelemetrySuite extends SharedSQLContext {
  var server: HttpServer = _

  override def beforeEach(): Unit = {
    server = HttpServer.create(new InetSocketAddress(8091), 0);
    server.createContext("/test", TestHttpHandler)
    server.start()
  }

  override def afterEach(): Unit = {
    server.stop(0)
  }

  test("test http post") {
    val httpClient = new HttpClientUtil
    val resp = httpClient.postJSON("http://127.0.0.1:8091/test", "msg")
    assert(resp.body.equals("test telemetry"))
  }

  test("test telemetry") {
    noException should be thrownBy {
      val telemetry = new Telemetry
      telemetry.setUrl("http://127.0.0.1:8091/test")
      val teleMsg = new TeleMsg(_sparkSession)
      telemetry.report(teleMsg)
    }
  }

  test("test get TiDB version") {
    val tiSparkTeleInfo = TiSparkTeleInfo.getTiSparkTeleInfo()
    assert(!tiSparkTeleInfo.get("tidb_version").contains("unknown"))
  }
}

object TestHttpHandler extends HttpHandler {
  override def handle(httpExchange: HttpExchange): Unit = {
    val response = "test telemetry"
    httpExchange.sendResponseHeaders(200, 0)
    val os = httpExchange.getResponseBody
    os.write(response.getBytes("UTF-8"))
    os.close()
  }
}
