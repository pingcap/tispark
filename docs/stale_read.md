# TiSpark Stable Read Feature

> Stale Read is a feature that can read historical versions of data stored in TiKV

## Feature
- TiSpark Provides Stale Read in session level
- Stale Read can read not only the historical data but also the historical schema
- Stale read only affects Spark-SQL. DataSource API will not be affected, such as `df.write` and `spark.read`

## New configuration
TiSpark support stale read with a new configuration `spark.tispark.stale_read`
- The configurations accept timestamp (ms) which is Long type
- The configuration better meet this condition `${now} - ${spark.tispark.stale_read} + ${sql execution time}) < ${GC lifetime}`, or you may get unexception data

## Limitation
- We suggest you don't use DML with stale read, for they need current schema rather than historical schema
- Stale read is not thread safety in a SparkSession, but you can execute SELECT statement concurrently in different SparkSession

## How to use

#### use with config
- config `spark.tispark.stale_read` in spark-default.conf
- or you can config when you submit application `spark-submit --conf spark.tispark.stale_read=1651766410000`

#### use with java/scala code
you can config `spark.tispark.stale_read` with different time before every SQL. To quit stale read, just config with `spark.conf.unset("spark.tispark.stale_read")` or `spark.conf.set("spark.tispark.stale_read", "")`
```
val spark = SparkSession.builder.config(sparkConf).getOrCreate()
spark.conf.set("spark.tispark.stale_read", 1651766410000L) //"2022-05-06 00:00:10"
spark.sql("select * from test.t")

spark.conf.set("spark.tispark.stale_read", 1651766420000L) //"2022-05-06 00:00:20"
spark.sql("select * from test.t")

spark.conf.set("spark.tispark.stale_read", "")
spark.sql("select * from test.t")
```
