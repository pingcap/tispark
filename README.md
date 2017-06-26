# tispark
A cannot-be-more-trivial implementation of tidb-spark-connector.


Uses as below
```
./spark-shell --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

import com.pingcap.tispark._
import org.apache.spark.sql.TiContext
import org.apache.spark.SparkConf
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
val ti = new TiContext(sqlContext.sparkSession)
val df = ti.tidbTable(List("127.0.0.1:" + 2379), "test", "t1")
df.createGlobalTempView("t1")
spark.sql("select avg(c1) from global_temp.t1").show

