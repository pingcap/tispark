# tispark
A cannot-be-more-trivial implementation of tidb-spark-connector.

Uses as below
```
./spark-shell --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

import com.pingcap.tispark._
import org.apache.spark.sql.TiContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
val ti = new TiContext(sqlContext.sparkSession)

val df = sqlContext.tidbTable(List("127.0.0.1:" + 2379), "test", "people")

df.createGlobalTempView("people")
spark.sql("select * from global_temp.people").show
```
