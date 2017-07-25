# tispark
Running Spark SQL on top of TiDB / TiKV.

A thin layer of TiSpark. Most of the logic is inside tikv-java-client library.
https://github.com/pingcap/tikv-client-lib-java


Uses as below
```
./bin/spark-shell --jars /wherever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

```

import org.apache.spark.sql.TiContext

// suppose your TiDB's Placement Driver is 127.0.0.1:2379
val ti = new TiContext(spark, List("127.0.0.1:" + 2379)) 

// Mapping all TiDB tables from database tpch as Spark SQL tables
ti.tidbMapDatabase("tpch")

spark.sql("select count(*) from lineitem").show
```
