package com.pingcap.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TiContext;

public class App {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .appName("TiSpark Application")
            .getOrCreate();

    TiContext ti = new TiContext(spark);
    ti.tidbMapDatabase("tpch", false);
    Dataset dataset = spark.sql("select count(*) from customer");
    dataset.show();
  }
}
