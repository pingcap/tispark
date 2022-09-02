# TiSpark (version >= 2.0) User Guide

> **Note:**
>
> This is a user guide for TiSpark version >= 2.0. If you are using version earlier than 2.0, refer to [Document for Spark 2.1](./userguide_spark2.1.md)

This document introduces how to set up and use TiSpark, which requires some basic knowledge of Apache Spark. Refer to [Spark website](https://spark.apache.org/docs/latest/index.html) for details.

# TOC
* [Overview](#overview)
* [Prerequisites for setting up TiSpark](#prerequisites-for-setting-up-tispark)
* [Recommended deployment configurations](#recommended-deployment-configurations)
   + [For independent deployment of Spark cluster and TiSpark cluster](#for-independent-deployment-of-spark-cluster-and-tispark-cluster)
   + [For hybrid deployment of TiSpark and TiKV cluster](#for-hybrid-deployment-of-tispark-and-tikv-cluster)
* [Getting Started](#getting-started)
* [Configuration](#configuration)
* [Write with TiSpark](#write-with-tispark)
* [Features](#features)
  * [Expression Index](#expression-index)
  * [TiFlash](#partition-table-support)
  * [Partition Table support](#partition-table-support)
  * [Other features](#other-features)
* [Statistics information](#statistics-information)
* [FAQ](#faq)
   
## Overview

TiSpark is a thin layer built for running Apache Spark on top of TiDB/TiKV to answer the complex OLAP queries. While enjoying the merits of both the Spark platform and the distributed clusters of TiKV, it is seamlessly integrated with TiDB, the distributed OLTP database, and thus blessed to provide one-stop Hybrid Transactional/Analytical Processing (HTAP) solutions for online transactions and analyses.

It is an OLAP solution that runs Spark SQL directly on TiKV, the distributed storage engine.

The figure below show the architecture of TiSpark.

![image alt text](architecture.png)

+ TiSpark integrates well with the Spark Catalyst Engine. It provides precise control of computing, which allows Spark to read data from TiKV efficiently. It also supports index seek, which significantly improves the performance of the point query execution.
+ It utilizes several strategies to push down computing to reduce the size of dataset handling by Spark SQL, which accelerates query execution. It also uses the TiDB built-in statistical information for the query plan optimization.
+ From the perspective of data integration, TiSpark + TiDB provides a solution that performs both transaction and analysis directly on the same platform without building and maintaining any ETLs. It simplifies the system architecture and reduces the cost of maintenance.
+ In addition, you can deploy and utilize the tools from the Spark ecosystem for further data processing and manipulation on TiDB. For example, using TiSpark for data analysis and ETL, retrieving data from TiKV as a data source for machine learning, generating reports from the scheduling system and so on.

TiSpark relies on the availability of TiKV clusters and PDs. You also need to set up and use the Spark clustering platform.

## Prerequisites for setting up TiSpark

+ The current TiSpark version supports Spark 2.3.x/2.4.x/3.0.x/3.1.x, but does not support any Spark versions earlier than 2.3.
+ TiSpark requires JDK 1.8+ and Scala 2.11/2.12.
+ TiSpark runs in any Spark mode such as `YARN`, `Mesos`, and `Standalone`.

## Recommended deployment configurations

### For independent deployment of Spark cluster and TiSpark cluster

Refer to the [Spark official website](https://spark.apache.org/docs/latest/hardware-provisioning.html) for detailed hardware recommendations.

+ It is recommended to allocate 32G memory for Spark. Reserve at least 25% of the memory for the operating system and the buffer cache.

+ It is recommended to provision at least 8 to 16 cores per machine for Spark. First, you must assign all the CPU cores to Spark.

The following is an example based on the `spark-env.sh` configuration:

```
SPARK_EXECUTOR_MEMORY = 32g
SPARK_WORKER_MEMORY = 32g
SPARK_WORKER_CORES = 8
```

Add the following lines in `spark-defaults.conf`.

```
spark.tispark.pd.addresses ${your_pd_servers}
spark.sql.extensions org.apache.spark.sql.TiExtensions
```

In the first line above, `your_pd_servers` is the PD addresses separated by commas, each in the format of `$your_pd_address:$port`.
For example, `10.16.20.1:2379,10.16.20.2:2379,10.16.20.3:2379`, which means that you have multiple PD servers on `10.16.20.1,10.16.20.2,10.16.20.3` with the port `2379`.

For TiSpark version >= 2.5.0, please add the following additional configuration to enable `Catalog` provided by `spark-3.0`.
```
spark.sql.catalog.tidb_catalog  org.apache.spark.sql.catalyst.catalog.TiCatalog`
spark.sql.catalog.tidb_catalog.pd.addresses  ${your_pd_adress}
```

### For hybrid deployment of TiSpark and TiKV cluster

For the hybrid deployment of TiSpark and TiKV, add the resources required by TiSpark to the resources reserved in TiKV, and allocate 25% of the memory for the system.

## Getting Started

See [Getting Started](https://github.com/pingcap/tispark/wiki/Getting-Started)

## Configuration

You can find all the configuration items in the [configuration](./configuration.md) file.

## Write with TiSpark

TiSpark natively supports writing data to TiKV via Spark Data Source API and guarantees ACID.
See [Data Source API User Guide](./datasource_api_userguide.md) for more detail.

TiSpark>=3.0.2 also supports Insert SQL. See [insert SQL](./insert_sql_userguide.md) for more detail.

You can also write to TiDB with Spark JDBC, this is the native way to use spark, no need to import TiSpark.

Here is a reference:

```scala
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

val customer = spark.sql("select * from customer limit 100000")
// you might repartition source to make it balanced across nodes
// and increase concurrency
val df = customer.repartition(32)
df.write
.mode(saveMode = "append")
.format("jdbc")
.option("driver", "com.mysql.jdbc.Driver")
 // replace the host and port with yours and be sure to use rewrite batch
.option("url", "jdbc:mysql://127.0.0.1:4000/test?rewriteBatchedStatements=true")
.option("useSSL", "false")
// as tested, setting to `150` is a good practice
.option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 150)
.option("dbtable", s"cust_test_select") // database name and table name here
.option("isolationLevel", "NONE") // set isolationLevel to NONE
.option("user", "root") // TiDB user here
.save()
```

Please set `isolationLevel` to `NONE` to avoid large single transactions which might lead to TiDB OOM and also avoid the `ISOLATION LEVEL does not support` error (TiDB currently only supports `REPEATABLE-READ`).


## Features

> Main Features

| Feature Support                   | TiSpark 2.4.x | TiSpark 2.5.x | TiSpark 3.0.x | TiSpark master |
| --------------------------------- | ------------- | ------------- | ----------- | -------------- |
| SQL select without tidb_catalog   | ✔           | ✔           |             |                |
| SQL select with tidb_catalog      |               | ✔           | ✔         | ✔            |
| DataFrame append                  | ✔           | ✔           | ✔         | ✔            |
| DataFrame reads                   | ✔           | ✔           | ✔         | ✔            |
| SQL show databases                | ✔           | ✔           | ✔         | ✔            |
| SQL show tables                   | ✔           | ✔           | ✔         | ✔            |
| SQL auth                          |               | ✔           | ✔         | ✔            |
| SQL delete from with tidb_catalog |               |               | ✔         | ✔            |
| TLS                               |               |               | ✔         | ✔            |
| DataFrame auth                    |               |               |             | ✔            |

### Expression Index

`tidb-5.0` supports Expression Index.

TiSpark currently supports retrieving data from table with `Expression Index`, but the `Expression Index` will not be used by the planner of TiSpark.

### TiFlash
TiSpark can read from TiFlash with the configuration `spark.tispark.isolation_read_engines`


### Partition Table support
 
**Reading partition table from TiDB**

TiSpark reads the range and hash partition table from TiDB.

Currently, TiSpark doesn't support a MySQL/TiDB partition table syntax `select col_name from table_name partition(partition_name)`, but you can still use `where` condition to filter the partitions.

TiSpark decides whether to apply partition pruning according to the partition type and the partition expression associated with the table.

Currently, TiSpark partially apply partition pruning on range partition.

The partition pruning is applied when the partition expression of the range partition is one of the following:

+ column expression
+ `YEAR($argument)` where the argument is a column and its type is datetime or string literal
  that can be parsed as datetime.

If partition pruning is not applied, TiSpark's reading is equivalent to doing a table scan over all partitions.

**Write into partition table**

Currently, TiSpark only supports writing into the range and hash partition table under the following conditions:
+ the partition expression is column expression
+ the partition expression is `YEAR($argument)` where the argument is a column and its type is datetime or string literal
  that can be parsed as datetime.

There are two ways to write into partition table:
1. Use datasource API to write into partition table which supports replace and append semantics.
2. Use delete statement with Spark SQL.

> [!NOTE]
> Currently the charset only supported is utf8mb4 and [`new_collations_enabled_on_first_bootstrap`](https://docs.pingcap.com/tidb/dev/tidb-configuration-file#new_collations_enabled_on_first_bootstrap)
> need to be set to `false` in TiDB.


### Other Features
- [Push down](./push_down.md)
- [Delete with TiSpark](./delete_userguide.md)
- [Stale read](https://github.com/pingcap/tispark/blob/master/docs/stale_read.md)
- [Authorization and authentication](https://github.com/pingcap/tispark/blob/master/docs/authorization_userguide.md)
- [TiSpark with multiple catalogs](https://github.com/pingcap/tispark/wiki/TiSpark-with-multiple-catalogs)
- [TiSpark TLS](https://github.com/pingcap/tispark/blob/master/docs/configuration.md#tls-notes)
- [TiSpark Telemetry](https://github.com/pingcap/tispark/blob/master/docs/telemetry.md)
- [TiSpark plan](./query_execution_plan_in_TiSpark.md)

### Statistics information

TiSpark uses the statistic information for:

+ Determining which index to use in your query plan with the lowest estimated cost.
+ Small table broadcasting, which enables efficient broadcast join.

For TiSpark to use the statistic information, first make sure that relevant tables have been analyzed.

See [here](https://github.com/pingcap/docs/blob/master/statistics.md) for more details about how to analyze tables.

Since TiSpark 2.0, statistics information is default to auto-load.

## FAQ

See our wiki [TiSpark FAQ](https://github.com/pingcap/tispark/wiki/TiSpark-FAQ)
