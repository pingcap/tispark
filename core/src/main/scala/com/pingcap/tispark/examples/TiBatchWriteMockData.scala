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

package com.pingcap.tispark.examples

import com.pingcap.tispark.{TiBatchWrite, TiDBOptions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, TiContext}

object TiBatchWriteMockData {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      throw new Exception("wrong arguments!")
    }

    // tpch_test LINEITEM tpch_test MOCK_LINEITEM
    // TPCH_1G LINEITEM TPCH_1G  MOCK_LINEITEM
    // TPCH_10G LINEITEM TPCH_10G  MOCK_LINEITEM
    // TPCH_100G LINEITEM TPCH_100G  MOCK_LINEITEM
    val inputDatabase = args(0)
    val inputTable = args(1)
    val outputDatabase = args(2)
    val outputTable = args(3)
    val enableRegionPreSplit = if (args.length >= 5) args(4).toBoolean else true
    val regionSplitNumber = if (args.length >= 6) Some(args(5).toInt) else None
    val tikvInstanceNumber = 3

    // init
    val start = System.currentTimeMillis()
    val sparkConf = new SparkConf()
      .setIfMissing("spark.tispark.write.enable", "true")
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", "172.16.30.81")
      .setIfMissing("tidb.port", "4000")
      .setIfMissing("tidb.user", "root")
      .setIfMissing("tidb.password", "")
      .setIfMissing("spark.tispark.pd.addresses", "172.16.30.81:2379")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val ti = new TiContext(spark)

    // select
    spark.sql("show databases").show()

    spark.sql(s"use $inputDatabase")
    var df = spark.sql(s"select * from $inputTable")

    for (i <- 1 to 184) {
      import org.apache.spark.sql.functions._
      df = df.withColumn(s"c$i", rand())
    }

    // batch write
    val options = new TiDBOptions(
      sparkConf.getAll.toMap ++ Map("database" -> outputDatabase, "table" -> outputTable)
    )
    TiBatchWrite.writeToTiDB(
      df,
      ti,
      options,
      regionSplitNumber,
      enableRegionPreSplit,
      Some(tikvInstanceNumber)
    )

    // time
    val end = System.currentTimeMillis()
    val seconds = (end - start) / 1000
    println(s"total time: $seconds seconds")
  }
}

/*
CREATE TABLE `MOCK_LINEITEM` (
  `L_ORDERKEY1` int(11) NOT NULL,
  `L_PARTKEY1` int(11) NOT NULL,
  `L_SUPPKEY1` int(11) NOT NULL,
  `L_LINENUMBER1` int(11) NOT NULL,
  `L_QUANTITY1` decimal(15,2) NOT NULL,
  `L_EXTENDEDPRICE1` decimal(15,2) NOT NULL,
  `L_DISCOUNT1` decimal(15,2) NOT NULL,
  `L_TAX1` decimal(15,2) NOT NULL,
  `L_RETURNFLAG1` char(1) NOT NULL,
  `L_LINESTATUS1` char(1) NOT NULL,
  `L_SHIPDATE1` date NOT NULL,
  `L_COMMITDATE1` date NOT NULL,
  `L_RECEIPTDATE1` date NOT NULL,
  `L_SHIPINSTRUCT1` char(25) NOT NULL,
  `L_SHIPMODE1` char(10) NOT NULL,
  `L_COMMENT1` varchar(44) NOT NULL,
  c1 double,
c2 double,
c3 double,
c4 double,
c5 double,
c6 double,
c7 double,
c8 double,
c9 double,
c10 double,
c11 double,
c12 double,
c13 double,
c14 double,
c15 double,
c16 double,
c17 double,
c18 double,
c19 double,
c20 double,
c21 double,
c22 double,
c23 double,
c24 double,
c25 double,
c26 double,
c27 double,
c28 double,
c29 double,
c30 double,
c31 double,
c32 double,
c33 double,
c34 double,
c35 double,
c36 double,
c37 double,
c38 double,
c39 double,
c40 double,
c41 double,
c42 double,
c43 double,
c44 double,
c45 double,
c46 double,
c47 double,
c48 double,
c49 double,
c50 double,
c51 double,
c52 double,
c53 double,
c54 double,
c55 double,
c56 double,
c57 double,
c58 double,
c59 double,
c60 double,
c61 double,
c62 double,
c63 double,
c64 double,
c65 double,
c66 double,
c67 double,
c68 double,
c69 double,
c70 double,
c71 double,
c72 double,
c73 double,
c74 double,
c75 double,
c76 double,
c77 double,
c78 double,
c79 double,
c80 double,
c81 double,
c82 double,
c83 double,
c84 double,
c85 double,
c86 double,
c87 double,
c88 double,
c89 double,
c90 double,
c91 double,
c92 double,
c93 double,
c94 double,
c95 double,
c96 double,
c97 double,
c98 double,
c99 double,
c100 double,
c101 double,
c102 double,
c103 double,
c104 double,
c105 double,
c106 double,
c107 double,
c108 double,
c109 double,
c110 double,
c111 double,
c112 double,
c113 double,
c114 double,
c115 double,
c116 double,
c117 double,
c118 double,
c119 double,
c120 double,
c121 double,
c122 double,
c123 double,
c124 double,
c125 double,
c126 double,
c127 double,
c128 double,
c129 double,
c130 double,
c131 double,
c132 double,
c133 double,
c134 double,
c135 double,
c136 double,
c137 double,
c138 double,
c139 double,
c140 double,
c141 double,
c142 double,
c143 double,
c144 double,
c145 double,
c146 double,
c147 double,
c148 double,
c149 double,
c150 double,
c151 double,
c152 double,
c153 double,
c154 double,
c155 double,
c156 double,
c157 double,
c158 double,
c159 double,
c160 double,
c161 double,
c162 double,
c163 double,
c164 double,
c165 double,
c166 double,
c167 double,
c168 double,
c169 double,
c170 double,
c171 double,
c172 double,
c173 double,
c174 double,
c175 double,
c176 double,
c177 double,
c178 double,
c179 double,
c180 double,
c181 double,
c182 double,
c183 double,
c184 double
)
 */
