## What is TiSpark?

TiSpark is a thin layer built for running Apache Spark on top of TiDB/TiKV. It takes advantages of the Spark, at the same time, seamlessly glues to the distributed OLTP database TiDB to provide a Hybrid Transaction/Analytical Processing (HTAP) solution answering complex OLAP queries. 

## Architecture

![architecture](./docs/architecture.png)

- TiSpark Integrates with Spark Catalyst Engine deeply, it provides precise control of the computing, which allows Spark read data from the storage of TiKV efficiently, It also supports index seek, which improves the performance of a point query execution significantly.

- It utilizes several of computing strategies to reduce both amount of computing tasks and size of data set handling by Spark SQL, which accelerates the query execution. It also uses TiDB built-in statistical information as a query plan optimization.

- From data integration point of view, TiSpark + TiDB provides a solution runs both transaction and analysis directly on the same platform without building and maintaining any ETLs. It simplifies system architecture and reduces the cost of maintenance.

- In addition, user can deploy and utilize tools from Spark ecosystem for further data processing and manipulation on TiDB . for example, using TiSpark for data analysis and ETL; retrieving data from TiKV as a machine learning data source; generating reports from  and so on.




TiSpark depends on the existence of TiKV clusters and PDs. It also needs to setup a Spark clustering platform. This document provides a quick introduction of how to setup and use TiSpark. It requires some basic knowledge of Apache Spark. For more information, please refer to Spark website (https://spark.apache.org/docs/latest/index.html).


- __Horizontal scalability__
Grow TiDB as your business grows. You can increase the capacity simply by adding more machines.

- __Asynchronous schema changes__
Evolve TiDB schemas as your requirement evolves. You can add new columns and indices without stopping or affecting the on-going operations.

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
