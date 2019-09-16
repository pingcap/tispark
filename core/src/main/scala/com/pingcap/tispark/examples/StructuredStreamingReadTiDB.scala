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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *
 * Usage: StructuredStreamingReadTiDB <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 */
object StructuredStreamingReadTiDB {

  case class ID(id: Int)

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", "localhost")
      .setIfMissing("tidb.port", "4000")
      .setIfMissing("tidb.user", "root")
      .setIfMissing("spark.tispark.pd.addresses", "localhost:2379")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" ")).map(s => ID(s.toInt))
    words.createOrReplaceGlobalTempView("streaming_test")

    val df =
      spark.sql(
        "select * from global_temp.streaming_test a join tpch_test.CUSTOMER b on a.id=b.C_CUSTKEY"
      )

    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    /*while (true) {
      Thread.sleep(10000)
      query.explain(true)
    }*/

    query.awaitTermination()
  }
}
