# tispark
A thin layer of TiSpark. Most of the logic is inside tikv-java-client library.
https://github.com/pingcap/tikv-client-lib-java


Uses as below
```
./spark-shell --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

import com.pingcap.tispark._
import org.apache.spark.sql.TiContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
val ti = new TiContext(sqlContext.sparkSession, List("127.0.0.1:" + 2379))

ti.tidbMapDatabase("tpch")
