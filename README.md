# tispark
A cannot-be-more-trivial implementation of tidb-spark-connector.

Uses as below
```
./spark-shell --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

import com.pingcap.tispark._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val df = sqlContext.tidbTable(List("127.0.0.1:" + 2379), "test", "t2")
df.collect()
```
