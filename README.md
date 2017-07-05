# tispark
A cannot-be-more-trivial implementation of tidb-spark-connector.


Uses as below
```
./spark-shell --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

import com.pingcap.tispark._
import org.apache.spark.sql.TiContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
val ti = new TiContext(sqlContext.sparkSession, List("127.0.0.1:" + 2379))


val df = ti.tidbTable("global_temp", "item")
df.createGlobalTempView("item")

spark.sql("select sum(discount) as avg_disc from global_temp.item").show

spark.sql("select count(*) as avg_disc from global_temp.item").show

