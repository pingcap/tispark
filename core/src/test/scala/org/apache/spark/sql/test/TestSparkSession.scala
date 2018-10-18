/*
 * Copyright 2017 PingCAP, Inc.
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

package org.apache.spark.sql.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * A special [[SparkSession]] prepared for testing.
 */
private[spark] class TestSparkSession(sparkConf: SparkConf) { self =>
  private val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("tispark-integration-test")
    .config(sparkConf.set("spark.sql.testkey", "true"))
    .getOrCreate()
  SparkSession.setDefaultSession(spark)
  SparkSession.setActiveSession(spark)

  def session: SparkSession = spark
}
