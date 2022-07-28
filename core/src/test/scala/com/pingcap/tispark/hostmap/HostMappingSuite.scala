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
package com.pingcap.tispark.hostmap

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tikv.hostmap.UriHostMapping
import com.pingcap.tispark.TiConfigConst.PD_ADDRESSES
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.catalog.TiCatalog
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper, have, noException, the}

class HostMappingSuite extends FunSuite {
  val HOST_MAPPING: String = "spark.tispark.host_mapping"

  test("Host map configuration test") {
    val conf: SparkConf = new SparkConf(false)
    conf.set(HOST_MAPPING, "host1:1;host2:2;host3:3")
    val tiConf: TiConfiguration = new TiConfiguration()
    TiUtil.sparkConfToTiConfWithoutPD(conf, tiConf)
    assert(tiConf.getHostMapping.asInstanceOf[UriHostMapping].getHostMapping.get("host1") == "1")
    assert(tiConf.getHostMapping.asInstanceOf[UriHostMapping].getHostMapping.size() == 3)
  }

  test("Host map configuration with empty") {
    val conf: SparkConf = new SparkConf(false)
    conf.set(HOST_MAPPING, "")
    val tiConf: TiConfiguration = new TiConfiguration()
    TiUtil.sparkConfToTiConfWithoutPD(conf, tiConf)
    assert(tiConf.getHostMapping.asInstanceOf[UriHostMapping].getHostMapping == null)
  }

  test("Host map configuration test with wrong host map") {
    the[IllegalArgumentException] thrownBy {
      val conf: SparkConf = new SparkConf(false)
      conf.set(HOST_MAPPING, "host1=1;host2=2;host3=3")
      val tiConf: TiConfiguration = new TiConfiguration()
      TiUtil.sparkConfToTiConfWithoutPD(conf, tiConf)
    } should have message "Invalid host mapping string: " + "host1=1;host2=2;host3=3"
  }

  test("Host map connection test") {
    noException should be thrownBy {
      var sc: SparkContext = null
      try {
        val conf: SparkConf = new SparkConf(false)
        val pdAddresses = "127.0.0.1"
        val fakePdAddresses = "127.1.1.1"
        val port = ":2379"
        conf.set(PD_ADDRESSES, fakePdAddresses + port)
        conf.set("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
        conf.set("spark.sql.catalog.tidb_catalog", TiCatalog.className)
        conf.set("spark.sql.catalog.tidb_catalog.pd.addresses", fakePdAddresses + port)
        conf.set(HOST_MAPPING, fakePdAddresses + ":" + pdAddresses)
        sc = new SparkContext("local[4]", "host-mapping-test", conf)
        val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
        spark.sql("use tidb_catalog")
        spark.sql("show databases").show()
      } finally {
        if (sc != null) {
          sc.stop()
          sc = null
        }
      }
    }
  }
}
