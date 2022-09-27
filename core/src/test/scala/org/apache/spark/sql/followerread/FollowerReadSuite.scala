/*
 * Copyright 2020 PingCAP, Inc.
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

package org.apache.spark.sql.followerread

import com.pingcap.tikv.{ClientSession, TiConfiguration}
import org.apache.spark.sql.BaseTiSparkTest
import org.scalatest.Matchers.{be, noException}

// This test needs 3 tikv with
// 1. address {127.0.0.1:20160,127.0.0.1:20161,127.0.0.1:20162}
// 2. label {key1=value1,key2=value2}
// 3. pd's config replication.max-replicas = 3
class FollowerReadSuite extends BaseTiSparkTest {

  val table = "test.follower_read_test"

  override def beforeAll(): Unit = {
    super.beforeAllWithoutLoadData()
    tidbStmt.execute("set config pd `replication.max-replicas` = 3")
    Thread.sleep(3000)
    ti.clientSession.close()
    tidbStmt.execute(s"drop table if exists $table")
    tidbStmt.execute(s"create table $table (c int)")
    tidbStmt.execute(s"insert into $table values (0)")
  }

  override def afterAll(): Unit = {
    tidbStmt.execute(s"drop table if exists $table")
    super.afterAll()
  }

  test("follower read basic") {
    judgeCancel
    // follower read is session level, we also need to close clientSession to clean the cache
    val spark_new = spark.newSession()
    spark_new.sparkContext.conf.set("spark.tispark.replica_read", "follower")
    noException should be thrownBy spark_new
      .sql(s"select * from tidb_catalog.$table")
      .show()
    val config = TiConfiguration.createDefault(pdAddresses)
    ClientSession.getInstance(config).close()
  }

  test("follower read with valid label") {
    judgeCancel
    val spark_new = spark.newSession()
    spark_new.sparkContext.conf.set("spark.tispark.replica_read.label", "key1=value1")
    spark_new.sparkContext.conf.set("spark.tispark.replica_read", "follower")
    noException should be thrownBy spark_new
      .sql(s"select * from tidb_catalog.$table")
      .show()
    val config = TiConfiguration.createDefault(pdAddresses)
    ClientSession.getInstance(config).close()
  }

  test("follower read with invalid label should throw exception") {
    judgeCancel
    val spark_new = spark.newSession()
    spark_new.sparkContext.conf.set("spark.tispark.replica_read.label", "key1=value2")
    spark_new.sparkContext.conf.set("spark.tispark.replica_read", "follower")
    assertThrows[Exception] {
      spark_new.sql(s"select * from tidb_catalog.$table").show
    }
    // need not close clientSession for it fail to init
    val config = TiConfiguration.createDefault(pdAddresses)
    ClientSession.getInstance(config).close()
  }

  test("follower read with label and whitelist") {
    judgeCancel
    val spark_new = spark.newSession()
    spark_new.sparkContext.conf.set("spark.tispark.replica_read.label", "key1=value2")
    spark_new.sparkContext.conf.set("spark.tispark.replica_read", "follower")
    // there are 3 replicas with 2 followers, 2 address can guarantee at least one follower is in whitelist
    spark_new.sparkContext.conf
      .set("spark.tispark.replica_read.address_whitelist", "127.0.0.1:20161,127.0.0.1:20162")
    noException should be thrownBy spark_new
      .sql(s"select * from tidb_catalog.$table")
      .show
    val config = TiConfiguration.createDefault(pdAddresses)
    ClientSession.getInstance(config).close()
  }

  test("follower read with label, whitelist and blacklist") {
    judgeCancel
    val spark_new = spark.newSession()
    spark_new.sparkContext.conf.set("spark.tispark.replica_read.label", "key1=value2")
    spark_new.sparkContext.conf.set("spark.tispark.replica_read", "follower")
    // there are 3 replicas with 2 followers, 2 address can guarantee at least one follower is in whitelist
    spark_new.sparkContext.conf
      .set("spark.tispark.replica_read.address_whitelist", "127.0.0.1:20161,127.0.0.1:20162")
    // put all the address into blacklist
    spark_new.sparkContext.conf.set(
      "spark.tispark.replica_read.address_blacklist",
      "127.0.0.1:20160,127.0.0.1:20161,127.0.0.1:20162")
    assertThrows[Exception] {
      spark_new.sql(s"select * from tidb_catalog.$table").show
    }
    val config = TiConfiguration.createDefault(pdAddresses)
    ClientSession.getInstance(config).close()
  }

  // this method will block this suit be tested in CI triggered by `/run-all-tests`
  def judgeCancel() {
    val resultSet = tidbStmt.executeQuery(
      "select count(*) as a from INFORMATION_SCHEMA.CLUSTER_INFO where type='tikv'")
    resultSet.next()
    if (resultSet.getInt(1) != 3) {
      cancel()
    }
  }
}
