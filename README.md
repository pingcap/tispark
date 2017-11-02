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


Uses as below
```
./bin/spark-shell --jars /wherever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

```

import org.apache.spark.sql.TiContext
val ti = new TiContext(spark) 

// Mapping all TiDB tables from database tpch as Spark SQL tables
ti.tidbMapDatabase("tpch")

spark.sql("select count(*) from lineitem").show
```

## Configuration

Below configurations can be put together with spark-defaults.conf or passed in the same way as other Spark config properties.

|    Key    | Default Value | Description |
| ---------- | --- | --- |
| spark.tispark.pd.addresses |  127.0.0.1:2379 | PD Cluster Addresses, split by comma |
| spark.tispark.grpc.framesize |  268435456 | Max frame size of GRPC response |
| spark.tispark.grpc.timeout_in_sec |  10 | GRPC timeout time in seconds |
| spark.tispark.meta.reload_period_in_sec |  60 | Metastore reload period in seconds |
| spark.tispark.plan.allowaggpushdown |  true | If allow aggregation pushdown (in case of busy TiKV nodes) |
| spark.tispark.index.scan_batch_size |  2000000 | How many row key in batch for concurrent index scan |
| spark.tispark.index.scan_concurrency |  5 | How many threads per index scan retrieving row keys |
| spark.tispark.table.scan_concurrency |  10 | How many threads per table scan |


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

