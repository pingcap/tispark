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

package com.pingcap.tispark.datasource

import com.pingcap.tikv.StoreVersion
import com.pingcap.tikv.allocator.RowIDAllocator
import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class AutoRandomSuite extends BaseBatchWriteTest("test_datasource_auto_random") {

  private val schema = StructType(List(StructField("i", LongType)))
  test("tispark overflow") {
    val shardBits = 15

    if (StoreVersion.isTiKVVersionGreatEqualThanVersion(this.ti.tiSession.getPDClient, "5.0.0")) {
      jdbcUpdate(
        s"create table $dbtable(i bigint primary key clustered NOT NULL AUTO_RANDOM($shardBits))")
    } else {
      if (isEnableAlterPrimaryKey) {
        cancel("TiDB config alter-primary-key must be false")
      }
      jdbcUpdate(s"create table $dbtable(i bigint primary key NOT NULL AUTO_RANDOM($shardBits))")
    }
    // TiSpark insert
    tidbWrite(
      List {
        Row(null)
      },
      schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = Math.pow(2, 64 - shardBits - 1).toLong - 3
    val allocator = allocateID(size, RowIDAllocator.RowIDAllocatorType.AUTO_RANDOM)

    // TiSpark insert
    tidbWrite(
      List {
        Row(null)
      },
      schema)

    // TiSpark insert overflow

    val caught = intercept[com.pingcap.tikv.exception.AllocateRowIDOverflowException] {
      tidbWrite(
        List {
          Row(null)
        },
        schema)
    }
    assert(caught.getMessage.startsWith("Overflow when allocating row id"))
  }

  test("tidb overflow") {
    val shardBits = 15

    if (StoreVersion.isTiKVVersionGreatEqualThanVersion(this.ti.tiSession.getPDClient, "5.0.0")) {
      jdbcUpdate(
        s"create table $dbtable(i bigint primary key clustered NOT NULL AUTO_RANDOM($shardBits))")
    } else {
      if (isEnableAlterPrimaryKey) {
        cancel("TiDB config alter-primary-key must be false")
      }
      jdbcUpdate(s"create table $dbtable(i bigint primary key NOT NULL AUTO_RANDOM($shardBits))")
    }

    // TiSpark insert
    tidbWrite(List(Row(null)), schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = Math.pow(2, 64 - shardBits - 1).toLong - 3
    val allocator = allocateID(size, RowIDAllocator.RowIDAllocatorType.AUTO_RANDOM)

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable values(null)")

    // TiDB insert overflow
    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable values(null)")
    }
    assert(caught.getMessage.equals("Failed to read auto-increment value from storage engine"))
  }

  test("test insert into unsigned auto_random") {
    val shardBits = 15

    if (StoreVersion.isTiKVVersionGreatEqualThanVersion(this.ti.tiSession.getPDClient, "5.0.0")) {
      jdbcUpdate(
        s"create table $dbtable(i bigint unsigned  primary key clustered NOT NULL AUTO_RANDOM($shardBits))")
    } else {
      if (isEnableAlterPrimaryKey) {
        cancel("TiDB config alter-primary-key must be false")
      }
      jdbcUpdate(
        s"create table $dbtable(i bigint unsigned  primary key NOT NULL AUTO_RANDOM($shardBits))")
    }

    // TiSpark insert
    tidbWrite(List(Row(null)), schema)
    assert(spark.sql(s"select * from $dbtable").count() == 1)
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "replace" -> "true",
      "database" -> database,
      "table" -> table)
    assert(spark.sql(s"select * from $dbtable").count() == 1)
    val df = spark.sql(s"select * from $dbtable")
    df.write.format("tidb").options(tidbOptions).mode("append").save()
    assert(spark.sql(s"select * from $dbtable").count() == 1)
    val rdd = sc.parallelize(Seq(Row(2L)))
    val row = sqlContext.createDataFrame(rdd, schema)
    val caught =
      intercept[TiBatchWriteException] { // Result type: IndexOutOfBoundsException
        row.write.format("tidb").options(tidbOptions).mode("append").save()
      }
    assert(
      caught.getMessage.equals(
        "currently user provided auto id value is only supported in update mode!"))
  }

  test("test insert into signed auto_random") {
    val shardBits = 1

    if (StoreVersion.isTiKVVersionGreatEqualThanVersion(this.ti.tiSession.getPDClient, "5.0.0")) {
      jdbcUpdate(
        s"create table $dbtable(i bigint primary key clustered NOT NULL AUTO_RANDOM($shardBits))")
    } else {
      if (isEnableAlterPrimaryKey) {
        cancel("TiDB config alter-primary-key must be false")
      }
      jdbcUpdate(s"create table $dbtable(i bigint primary key NOT NULL AUTO_RANDOM($shardBits))")
    }

    // TiSpark insert
    tidbWrite(List(Row(null)), schema)
    assert(spark.sql(s"select * from $dbtable").count() == 1)
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "replace" -> "true",
      "database" -> database,
      "table" -> table)
    assert(spark.sql(s"select * from $dbtable").count() == 1)
    val df = spark.sql(s"select * from $dbtable")
    df.write.format("tidb").options(tidbOptions).mode("append").save()
    assert(spark.sql(s"select * from $dbtable").count() == 1)
    val rdd = sc.parallelize(Seq(Row(2L)))
    val row = sqlContext.createDataFrame(rdd, schema)
    val caught =
      intercept[TiBatchWriteException] { // Result type: IndexOutOfBoundsException
        row.write.format("tidb").options(tidbOptions).mode("append").save()
      }
    assert(
      caught.getMessage.equals(
        "currently user provided auto id value is only supported in update mode!"))
  }

}
