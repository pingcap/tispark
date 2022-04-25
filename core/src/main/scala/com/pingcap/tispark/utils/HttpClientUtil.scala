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

package com.pingcap.tispark.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}

class HttpClientUtil {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * HTTP POST
   *
   * @param url    server url
   * @param msg    post entry object
   * @param headers HTTP header
   * @return HTTP response string
   */
  def postJSON(url: String, msg: Object, headers: Map[String, String] = null): HttpResponse[String] = {
    try {
      val mapper = new ObjectMapper()
        .registerModule(DefaultScalaModule)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      val msgString = mapper.writeValueAsString(msg)

      val header: Map[String, String] =
        if (headers != null) headers else Map("Content-Type" -> "application/json")

      val resp = Http(url).postData(msgString).headers(header).asString
      checkResp(resp)

      resp
    } catch {
      case e: Throwable =>
        logger.error("Failed to send HTTP POST request")
        throw e
    }
  }

  /**
   * HTTP GET
   *
   * @param url      server url
   * @param headers  HTTP header
   * @return HTTP response string
   */
  def get(url: String, headers: Map[String, String] = null): HttpResponse[String] = {
    try {
      val header: Map[String, String] =
        if (headers != null) headers else Map("Content-Type" -> "application/json")

      val resp = Http(url).headers(header).asString
      checkResp(resp)

      resp
    } catch {
      case e: Throwable =>
        logger.error("Failed to send HTTP GET request")
        throw e
    }
  }

  private def checkResp(resp: HttpResponse[String]): Unit = {
    if (!resp.isSuccess) {
      logger.error("failed to get HTTP request: %s, response: %s, code %d",
        resp.body,
        resp.code)
    }
  }
}