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

package org.apache.spark.sql.types.pk

import com.pingcap.tikv.util.ConvertUpstreamUtils
import org.apache.spark.sql.{BaseTiSparkTest, Row}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class ClusterIndexScanSuites extends BaseTiSparkTest {
  def isSupportCommonHandle(): Unit = {
    if (!ConvertUpstreamUtils.isTiKVVersionGreatEqualThanVersion(
        this.ti.clientSession.getTiKVSession.getPDClient,
        "5.0.0")) {
      cancel("TiDB version must bigger or equal than 5.0")
    }
  }

  test("test signal col cluster index scan") {
    isSupportCommonHandle()
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""
        |CREATE TABLE `t1` (
        |  `a` BIGINT(20) NOT NULL,
        |  `b` varchar(255) NOT NULL,
        |  `c` varchar(255) DEFAULT NULL,
        |   PRIMARY KEY (`b`) clustered
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)
    spark.sql("insert into t1 values(1,'1','1'),(2,'2','2'),(3,'3','3')")
    // without filter
    val all = spark.sql("select * from t1")
    assert(all.count() == 3)
    all.collect() should contain theSameElementsAs Array(
      Row(1, "1", "1"),
      Row(2, "2", "2"),
      Row(3, "3", "3"))
    // low no equal without up limit condition
    val lowNoEqualWithoutUp = spark.sql("select * from t1 where b>'1'")
    assert(lowNoEqualWithoutUp.count() == 2)
    lowNoEqualWithoutUp.collect() should contain theSameElementsAs Array(
      Row(2, "2", "2"),
      Row(3, "3", "3"))
    // low equal condition without up limit condition
    val lowEqualWithoutUp = spark.sql("select * from t1 where b>='1'")
    assert(lowEqualWithoutUp.count() == 3)
    lowEqualWithoutUp.collect() should contain theSameElementsAs Array(
      Row(1, "1", "1"),
      Row(2, "2", "2"),
      Row(3, "3", "3"))
    // up no equal without low limit condition
    val upNoEqualWithoutLow = spark.sql("select * from t1 where b<'3'")
    assert(upNoEqualWithoutLow.count() == 2)
    upNoEqualWithoutLow.collect() should contain theSameElementsAs Array(
      Row(1, "1", "1"),
      Row(2, "2", "2"))
    // up equal without low limit condition
    val upEqualWithoutLow = spark.sql("select * from t1 where b<='3'")
    assert(upEqualWithoutLow.count() == 3)
    upEqualWithoutLow.collect() should contain theSameElementsAs Array(
      Row(1, "1", "1"),
      Row(2, "2", "2"),
      Row(3, "3", "3"))
    // low no equal with up no equal condition
    val upNoEqualWithLowNoEqual = spark.sql("select * from t1 where b>'1' and b<'3'")
    assert(upNoEqualWithLowNoEqual.count() == 1)
    upNoEqualWithLowNoEqual.collect() should contain theSameElementsAs Array(Row(2, "2", "2"))
    // low equal with up equal condition
    val upEqualWithLowEqual = spark.sql("select * from t1 where b>='1' and b<='3'")
    assert(upEqualWithLowEqual.count() == 3)
    upEqualWithLowEqual.collect() should contain theSameElementsAs Array(
      Row(1, "1", "1"),
      Row(2, "2", "2"),
      Row(3, "3", "3"))
  }

  test("test multi col cluster index scan") {
    isSupportCommonHandle()
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""
        |CREATE TABLE `t1` (
        |  `a` BIGINT(20) NOT NULL,
        |  `b` varchar(255) NOT NULL,
        |  `c` varchar(255) DEFAULT NULL,
        |   PRIMARY KEY (`b`,`a`) clustered
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin)
    spark.sql("insert into t1 values(1,'1','1'),(2,'2','2'),(3,'3','3')")
    spark.sql("insert into t1 values(10,'1','1'),(20,'2','2'),(30,'3','3')")
    spark.sql("insert into t1 values(100,'1','1'),(200,'2','2'),(300,'3','3')")
    // first equal and second range
    val firstEqualSecondRange = spark.sql("select * from t1 where b='1' and a<100")
    assert(firstEqualSecondRange.count() == 2)
    firstEqualSecondRange.collect() should contain theSameElementsAs Array(
      Row(1, "1", "1"),
      Row(10, "1", "1"))
    // first equal and second equal
    val firstEqualSecondEqual = spark.sql("select * from t1 where b='1' and a=100")
    assert(firstEqualSecondEqual.count() == 1)
    firstEqualSecondEqual.collect() should contain theSameElementsAs Array(Row(100, "1", "1"))
    // second equal
    val secondEqual = spark.sql("select * from t1 where a=100")
    assert(firstEqualSecondEqual.count() == 1)
    firstEqualSecondEqual.collect() should contain theSameElementsAs Array(Row(100, "1", "1"))
    // second range
    val secondRange = spark.sql("select * from t1 where a<200")
    assert(secondRange.count() == 7)
    secondRange.collect() should contain theSameElementsAs Array(
      Row(1, "1", "1"),
      Row(10, "1", "1"),
      Row(100, "1", "1"),
      Row(2, "2", "2"),
      Row(20, "2", "2"),
      Row(3, "3", "3"),
      Row(30, "3", "3"))
    // first range and second range
    val firstRangeSecondRange = spark.sql("select * from t1 where b<='2' and a<200")
    assert(firstRangeSecondRange.count() == 5)
    firstRangeSecondRange.collect() should contain theSameElementsAs Array(
      Row(1, "1", "1"),
      Row(10, "1", "1"),
      Row(100, "1", "1"),
      Row(2, "2", "2"),
      Row(20, "2", "2"))
    // first range and second equal
    val firstRangeSecondEqual = spark.sql("select * from t1 where b<='2' and a=200")
    assert(firstRangeSecondEqual.count() == 1)
    firstRangeSecondEqual.collect() should contain theSameElementsAs Array(Row(200, "2", "2"))
  }
}
