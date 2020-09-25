/*
 * Copyright 2019 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark

import com.pingcap.tikv.exception.TiInternalException
import org.slf4j.LoggerFactory

object TiSparkInfo {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  val SUPPORTED_SPARK_VERSION: List[String] = "3.0" :: Nil

  val SPARK_VERSION: String = org.apache.spark.SPARK_VERSION

  val SPARK_MAJOR_VERSION: String = {
    SUPPORTED_SPARK_VERSION.find(SPARK_VERSION.startsWith).getOrElse("unknown")
  }
  val info: String = {
    s"""Supported Spark Version: ${SUPPORTED_SPARK_VERSION.mkString(" ")}
       |Current Spark Version: $SPARK_VERSION
       |Current Spark Major Version: $SPARK_MAJOR_VERSION""".stripMargin
  }

  def versionSupport(): Boolean = {
    TiSparkInfo.SUPPORTED_SPARK_VERSION.contains(SPARK_MAJOR_VERSION)
  }

  def checkVersion(): Unit = {
    logger.info(info)
    if (!versionSupport()) {
      logger.error("Current TiSpark Version is not compatible with current Spark Version!")
      throw new TiInternalException("")
    }
  }
}
