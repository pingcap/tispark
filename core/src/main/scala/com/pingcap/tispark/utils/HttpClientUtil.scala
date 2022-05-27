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
import com.pingcap.tikv.TiConfiguration
import com.pingcap.tispark.TiConfigConst
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions, HttpResponse}
import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.security.{KeyFactory, KeyStore, PrivateKey}
import java.security.cert.{Certificate, CertificateFactory}
import java.security.spec.PKCS8EncodedKeySpec
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import java.util.stream.Collectors
import java.util.Base64

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
  def postJSON(
      url: String,
      msg: Object,
      headers: Map[String, String] = null): HttpResponse[String] = {
    try {
      val mapper = new ObjectMapper()
        .registerModule(DefaultScalaModule)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      val msgString = mapper.writeValueAsString(msg)

      val header: Map[String, String] =
        if (headers != null) headers else Map("Content-Type" -> "application/json")

      val resp = Http(url).postData(msgString).headers(header).asString
      checkResp(url, resp)

      resp
    } catch {
      case e: Throwable =>
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
      checkResp(url, resp)

      resp
    } catch {
      case e: Throwable =>
        throw e
    }
  }

  /**
   * HTTPS GET
   *
   * Support PKCS#8 and JKS format.
   *
   * @param url      server url
   * @param conf     which contain certificate path
   * @param headers  HTTP header
   * @return HTTP response string
   */
  def getHttps(
      url: String,
      conf: TiConfiguration,
      headers: Map[String, String] = null): HttpResponse[String] = {
    try {
      val header: Map[String, String] =
        if (headers != null) headers else Map("Content-Type" -> "application/json")

      val kmf: KeyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      val tmf: TrustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      val keyStore = KeyStore.getInstance("JKS")
      val trustStore = KeyStore.getInstance("JKS")

      if (conf.isJksEnable) {
        val jksTrustPath = conf.getJksTrustPath
        val jksTrustPassword = conf.getJksTrustPassword
        val jksKeyPath = conf.getJksKeyPath
        val jksKeyPassword = conf.getJksKeyPassword

        keyStore.load(new FileInputStream(jksKeyPath), jksKeyPassword.toCharArray)
        kmf.init(keyStore, jksKeyPassword.toCharArray)

        trustStore.load(new FileInputStream(jksTrustPath), jksTrustPassword.toCharArray)
        tmf.init(trustStore)
      } else {
        val trustCertCollectionFilePath = conf.getTrustCertCollectionFile
        val keyCertChainFilePath = conf.getKeyCertChainFile
        val keyFilePath = conf.getKeyFile

        val key: String = getContent(new FileInputStream(keyFilePath))
        val privatekey: PrivateKey = pemLoadPkcs8(key)

        val trustCert = paraseCertificate(new FileInputStream(trustCertCollectionFilePath))

        val keyCert = paraseCertificate(new FileInputStream(keyCertChainFilePath))

        keyStore.load(null, null)
        keyStore.setKeyEntry(
          "key",
          privatekey,
          "PASSWORD".toCharArray,
          Array[Certificate](keyCert))
        kmf.init(keyStore, "PASSWORD".toCharArray)

        trustStore.load(null, null)
        trustStore.setCertificateEntry("cert", trustCert)
        tmf.init(trustStore)
      }

      val sslContext = SSLContext.getInstance("TLSv1.2")
      sslContext.init(kmf.getKeyManagers, tmf.getTrustManagers, null)

      val resp = Http(url)
        .headers(header)
        .option(HttpOptions.sslSocketFactory(sslContext.getSocketFactory))
        .asString
      checkResp(url, resp)

      resp
    } catch {
      case e: Throwable =>
        throw e
    }
  }

  private def checkResp(url: String, resp: HttpResponse[String]): Unit = {
    if (!resp.isSuccess) {
      logger.info(
        s"Failed to get HTTP request: ${url}, response: ${resp.body}, code ${resp.code}")
    }
  }

  private def getContent(inputStream: InputStream): String = {
    var inputStreamReader: InputStreamReader = null
    var bufferedReader: BufferedReader = null
    try {
      inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)
      bufferedReader = new BufferedReader(inputStreamReader)
      bufferedReader.lines.collect(Collectors.joining(System.lineSeparator))
    } catch {
      case e: Throwable =>
        throw e
    } finally {
      if (inputStreamReader != null) inputStreamReader.close()
      if (bufferedReader != null) bufferedReader.close()
    }
  }

  private def paraseCertificate(certificateStream: InputStream): Certificate = {
    try {
      val certificateFactory: CertificateFactory = CertificateFactory.getInstance("X.509")
      certificateFactory.generateCertificate(certificateStream)
    } catch {
      case e: Throwable =>
        throw e
    }
  }

  private def pemLoadPkcs8(key: String): PrivateKey = {
    try {
      val PEM_PRIVATE_START = "-----BEGIN PRIVATE KEY-----"
      val PEM_PRIVATE_END = "-----END PRIVATE KEY-----"
      val pureKey =
        key.replace(PEM_PRIVATE_START, "").replace(PEM_PRIVATE_END, "").replaceAll("\\s", "")
      val pkcs8EncodeKey = Base64.getDecoder.decode(pureKey)
      val keyFactory = KeyFactory.getInstance("RSA")
      keyFactory.generatePrivate(new PKCS8EncodedKeySpec(pkcs8EncodeKey))
    } catch {
      case e: Throwable =>
        throw e
    }
  }
}

object HttpClientUtil {
  def getTLSParam(conf: TiConfiguration): Unit = {
    try {
      val sqlConf = SparkSession.active.sessionState.conf.clone()
      val TLSEnable = sqlConf.getConfString(TiConfigConst.TIKV_TLS_ENABLE, "false").toBoolean
      if (TLSEnable) {
        conf.setTlsEnable(true)
        val jksEnable = sqlConf.getConfString(TiConfigConst.TIKV_JKS_ENABLE, "false").toBoolean
        if (jksEnable) {
          conf.setJksEnable(true)
          conf.setJksKeyPath(sqlConf.getConfString(TiConfigConst.TIKV_JKS_KEY_PATH))
          conf.setJksKeyPassword(sqlConf.getConfString(TiConfigConst.TIKV_JKS_KEY_PASSWORD))
          conf.setJksTrustPath(sqlConf.getConfString(TiConfigConst.TIKV_JKS_TRUST_PATH))
          conf.setJksTrustPassword(sqlConf.getConfString(TiConfigConst.TIKV_JKS_TRUST_PASSWORD))
        } else {
          conf.setTrustCertCollectionFile(
            sqlConf.getConfString(TiConfigConst.TIKV_TRUST_CERT_COLLECTION))
          conf.setKeyCertChainFile(sqlConf.getConfString(TiConfigConst.TIKV_KEY_CERT_CHAIN))
          conf.setKeyFile(sqlConf.getConfString(TiConfigConst.TIKV_KEY_FILE))
        }
      }
    } catch {
      case e: Throwable =>
        throw e
    }
  }
}
