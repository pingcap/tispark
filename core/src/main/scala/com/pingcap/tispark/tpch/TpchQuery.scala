package com.pingcap.tispark.tpch

import org.apache.spark.{sql, SparkConf, SparkContext}
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

import org.apache.spark.sql._

import scala.collection.mutable.ListBuffer

/**
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String =
    className.split("\\.").last.replaceAll("\\$", "")

  def getName: String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(spark: SparkSession): DataFrame
}

object TpchQuery {
  def executeQueries(spark: SparkSession,
                     db: String,
                     queryNum: Int): ListBuffer[(String, Float)] = {
    val results = new ListBuffer[(String, Float)]

    var fromNum = 1
    var toNum = 22
    if (queryNum != 0) {
      fromNum = queryNum
      toNum = queryNum
    }

    // change current db
    spark.sql(s"use $db")
    for (queryNo <- fromNum to toNum) {

      if (queryNo != 15) {
        val query =
          Class
            .forName(f"com.pingcap.tispark.tpch.Q$queryNo%02d")
            .newInstance
            .asInstanceOf[TpchQuery]
        val t0 = System.nanoTime()
        // print
        query.execute(spark).collect().foreach(println)

        val t1 = System.nanoTime()
        val elapsed = (t1 - t0) / 1000000000.0f // second
        results += Tuple2(query.getName, elapsed)
      }

    }

    results
  }

  def main(args: Array[String]): Unit = {

    var queryNum = 0
    var dbName = "tpch"

    if (args.length > 0)
      dbName = args(0).toString

    if (args.length > 1)
      queryNum = args(1).toInt

    val builder = new sql.SparkSession.Builder
    val spark = builder.getOrCreate()

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(spark, dbName, queryNum)

    val outFile = new File("times.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"$key%s\t$value%1.8f\n")
    }

    bw.close()
  }
}
