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

package org.apache.spark

import org.apache.spark.SharedSparkContext._
import org.apache.spark.sql.internal.StaticSQLConf
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait SharedSparkContext extends BeforeAndAfterAll with BeforeAndAfterEach { self: Suite =>

  protected var _isHiveEnabled: Boolean = false
  protected var conf: SparkConf = new SparkConf(false)

  def sc: SparkContext = _sc

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (_sc != null) {
      SharedSparkContext.stop()
    }
    initializeContext()
  }

  protected def initializeContext(): Unit =
    synchronized {
      if (null == _sc) {
        conf.set("spark.sql.test.key", "true")
        if (_isHiveEnabled) {
          conf.set(StaticSQLConf.CATALOG_IMPLEMENTATION, "hive")
        }
        _sc = new SparkContext("local[4]", "tispark-integration-test", conf)
      }
    }

  override protected def afterAll(): Unit = {
    try {
      SharedSparkContext.stop()
    } finally {
      super.afterAll()
    }
  }
}

object SharedSparkContext {

  @transient private var _sc: SparkContext = _

  def stop(): Unit =
    synchronized {
      if (_sc != null) {
        _sc.stop()
        _sc = null
      }
      // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
    }

}
