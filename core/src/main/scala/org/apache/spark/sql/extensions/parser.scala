/*
 * Copyright 2018 PingCAP, Inc.
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

package org.apache.spark.sql.extensions

import com.pingcap.tispark.utils.ReflectionUtil

import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.{SparkSession, TiContext, TiExtensions}

class TiParserFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends ((SparkSession, ParserInterface) => ParserInterface) {
  override def apply(
      sparkSession: SparkSession,
      parserInterface: ParserInterface): ParserInterface = {
    if (TiExtensions.catalogPluginMode(sparkSession)) {
      parserInterface
    } else {
      ReflectionUtil.newTiParser(getOrCreateTiContext, sparkSession, parserInterface)
    }
  }
}
