## What is TiSpark?

TiSpark is a thin layer built for running Apache Spark on top of TiDB/TiKV to answer the complex OLAP queries. It takes advantages of both the Spark platform and the distributed TiKV cluster, at the same time, seamlessly glues to TiDB, the distributed OLTP database, to provide a Hybrid Transactional/Analytical Processing (HTAP) to serve as a one-stop solution for online transactions and analysis.

## TiSpark Architecture

![architecture](./docs/architecture.png)


- TiSpark integrates with Spark Catalyst Engine deeply. It provides precise control of the computing, which allows Spark read data from TiKV efficiently. It also supports index seek, which improves the performance of the point query execution significantly.

- It utilizes several strategies to push down the computing to reduce the size of dataset handling by Spark SQL, which accelerates the query execution. It also uses the TiDB built-in statistical information for  the query plan optimization.

- From the data integration point of view, TiSpark + TiDB provides a solution runs both transaction and analysis directly on the same platform without building and maintaining any ETLs. It simplifies the system architecture and reduces the cost of maintenance.

- In addition, you can deploy and utilize tools from the Spark ecosystem for further data processing and manipulation on TiDB. For example, using TiSpark for data analysis and ETL; retrieving data from TiKV as a machine learning data source; generating reports from the scheduling system  and so on.

TiSpark depends on the existence of TiKV clusters and PDs. It also needs to setup and use Spark clustering platform. 

A thin layer of TiSpark. Most of the logic is inside tikv-java-client library.
https://github.com/pingcap/tikv-client-lib-java


## Usage
### For spark-shell
1. Download or compile tispark [here](https://github.com/pingcap/tispark).
2. ```cd``` to your spark configuration directory and add this line in your ```spark-default.conf```
```
spark.tispark.pd.addresses 127.0.0.1:2379
```
You can change your pd address according to your actual deployment environment.

3. ```cd``` to you spark home directory and run
```
./bin/spark-shell --jars /wherever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```
Then in your spark-shell console:
```scala
import org.apache.spark.sql.TiContext
val ti = new TiContext(spark) 

// Mapping all TiDB tables from database tpch as Spark SQL tables
ti.tidbMapDatabase("tpch")

spark.sql("select count(*) from lineitem").show
```
Result:
```
+--------+
|count(1)|
+--------+
|    6005|
+--------+
```

### For spark-submit

1. Download or compile tispark [here](https://github.com/pingcap/tispark) and make sure it is visible in your local [maven](maven.apache.org) repository.
2. In your project pom.xml, include these dependencies:
```xml
<dependencies>
    <dependency>
        <groupId>com.pingcap.tispark</groupId>
        <artifactId>tispark</artifactId>
        <version>${tispark.version}</version>
    </dependency>

    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-catalyst_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-hive_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-unsafe_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
</dependencies>

```  
3. Write your own application code. In this example, we create a Java file ```com.pingcap.spark.App``` like this:

```java
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
    Dataset dataset = spark.sql("select * from customer");
    dataset.show();
  }
}
```
We ues it to fetch all rows from table ```tpch.customer``` and print them out.

4. Run ```mvn clean package``` in your console to build ```your-application.jar```
5. ```cd``` to your spark configuration directory and add this line in your ```spark-default.conf```
```
spark.tispark.pd.addresses 127.0.0.1:2379
```
You can change your pd address according to your actual deployment environment.

6. ```cd``` to you spark home directory and run
```
./bin/spark-submit --class <main class> --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar your-application.jar
```
`--class` specifies the entry point of your application  
`--jars` specifies the TiSpark library   

In this case, we run
```
./bin/spark-submit --class com.pingcap.spark.App --jars /home/novemser/Documents/Code/PingCAP/tispark/target/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar /home/novemser/Documents/Code/Java/tisparksample/target/tispark-sample-0.1.0-SNAPSHOT.jar
```
And the results:
```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
17/10/26 19:58:27 INFO SparkContext: Running Spark version 2.2.0
17/10/26 19:58:28 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
17/10/26 19:58:28 WARN Utils: Your hostname, NPC resolves to a loopback address: 127.0.1.1; using 172.17.19.143 instead (on interface wlp2s0)
17/10/26 19:58:28 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
17/10/26 19:58:28 INFO SparkContext: Submitted application: TiSpark Application
17/10/26 19:58:28 INFO SecurityManager: Changing view acls to: novemser
17/10/26 19:58:28 INFO SecurityManager: Changing modify acls to: novemser
17/10/26 19:58:28 INFO SecurityManager: Changing view acls groups to: 
17/10/26 19:58:28 INFO SecurityManager: Changing modify acls groups to: 
17/10/26 19:58:28 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(novemser); groups with view permissions: Set(); users  with modify permissions: Set(novemser); groups with modify permissions: Set()
17/10/26 19:58:29 INFO Utils: Successfully started service 'sparkDriver' on port 46249.
17/10/26 19:58:29 INFO SparkEnv: Registering MapOutputTracker
17/10/26 19:58:29 INFO SparkEnv: Registering BlockManagerMaster
...dummy info ignored
17/10/26 19:58:37 INFO CodeGenerator: Code generated in 42.029035 ms
+---------+------------------+--------------------+-----------+---------------+---------+------------+--------------------+
|c_custkey|            c_name|           c_address|c_nationkey|        c_phone|c_acctbal|c_mktsegment|           c_comment|
+---------+------------------+--------------------+-----------+---------------+---------+------------+--------------------+
|        1|Customer#000000001|   IVhzIApeRb ot,c,E|         15|25-989-741-2988|   711.56|    BUILDING|to the even, regu...|
|        2|Customer#000000002|XSTf4,NCwDVaWNe6t...|         13|23-768-687-3665|   121.65|  AUTOMOBILE|l accounts. blith...|
|        3|Customer#000000003|        MG9kdTD2WBHm|          1|11-719-748-3364|  7498.12|  AUTOMOBILE| deposits eat sly...|
|        4|Customer#000000004|         XxVSJsLAGtn|          4|14-128-190-5944|  2866.83|   MACHINERY| requests. final,...|
|        5|Customer#000000005|KvpyuHCplrB84WgAi...|          3|13-750-942-6364|   794.47|   HOUSEHOLD|n accounts will h...|
|        6|Customer#000000006|sKZz0CsnMD7mp4Xd0...|         20|30-114-968-4951|  7638.57|  AUTOMOBILE|tions. even depos...|
|        7|Customer#000000007|TcGe5gaZNgVePxU5k...|         18|28-190-982-9759|  9561.95|  AUTOMOBILE|ainst the ironic,...|
|        8|Customer#000000008|I0B10bB0AymmC, 0P...|         17|27-147-574-9335|  6819.74|    BUILDING|among the slyly r...|
|        9|Customer#000000009|xKiAFTjUsCuxfeleN...|          8|18-338-906-3675|  8324.07|   FURNITURE|r theodolites acc...|
|       10|Customer#000000010|6LrEaV6KR6PLVcgl2...|          5|15-741-346-9870|  2753.54|   HOUSEHOLD|es regular deposi...|
|       11|Customer#000000011|PkWS 3HlXqwTuzrKg...|         23|33-464-151-3439|  -272.60|    BUILDING|ckages. requests ...|
|       12|Customer#000000012|       9PWKuhzT4Zr1Q|         13|23-791-276-1263|  3396.49|   HOUSEHOLD| to the carefully...|
|       13|Customer#000000013|nsXQu0oVjD7PM659u...|          3|13-761-547-5974|  3857.34|    BUILDING|ounts sleep caref...|
|       14|Customer#000000014|     KXkletMlL2JQEA |          1|11-845-129-3851|  5266.30|   FURNITURE|, ironic packages...|
|       15|Customer#000000015|YtWggXoOLdwdo7b0y...|         23|33-687-542-7601|  2788.52|   HOUSEHOLD| platelets. regul...|
|       16|Customer#000000016| cYiaeMLZSMAOQ2 d0W,|         10|20-781-609-3107|  4681.03|   FURNITURE|kly silent courts...|
|       17|Customer#000000017|izrh 6jdqtp2eqdtb...|          2|12-970-682-3487|     6.34|  AUTOMOBILE|packages wake! bl...|
|       18|Customer#000000018|3txGO AiuFux3zT0Z...|          6|16-155-215-1315|  5494.43|    BUILDING|s sleep. carefull...|
|       19|Customer#000000019|uc,3bHIx84H,wdrmL...|         18|28-396-526-5053|  8914.71|   HOUSEHOLD| nag. furiously c...|
|       20|Customer#000000020|       JrPk8Pqplj4Ne|         22|32-957-234-8742|  7603.40|   FURNITURE|g alongside of th...|
+---------+------------------+--------------------+-----------+---------------+---------+------------+--------------------+
only showing top 20 rows

```


## Configuration

Below configurations can be put together with spark-defaults.conf or passed in the same way as other Spark config properties.

|    Key    | Default Value | Description |
| ---------- | --- | --- |
| spark.tispark.pd.addresses |  (empty) | PD Cluster Addresses, split by comma |
| spark.tispark.grpc.framesize |  268435456 | Max frame size of GRPC response |
| spark.tispark.grpc.timeout_in_sec |  10 | GRPC timeout time in seconds |
| spark.tispark.meta.reload_period_in_sec |  60 | Metastore reload period in seconds |
| spark.tispark.plan.allowaggpushdown |  true | If allow aggregation pushdown (in case of busy TiKV nodes) |


## Quick start

Read the [Quick Start](./docs/userguide.md).

## How to build

TiSpark depends on TiKV java client project which is included as a submodule. 
To build TiKV client:
```
./bin/build-client.sh
```
To build TiSpark itself:
```
mvn clean package
```

## Follow us

### Twitter

[@PingCAP](https://twitter.com/PingCAP)

### Mailing list

tidb-user@googlegroups.com

[Google Group](https://groups.google.com/forum/#!forum/tidb-user)

## License
TiSpark is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

