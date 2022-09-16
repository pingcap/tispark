# TiSpark (version >= 2.0) User Guide

> **Note:**
>
> TiSpark 2.4 is the only version we still maintain in TiSpark 2.x.

- [Requirements](#requirements)
- [Recommended deployment configurations](#recommended-deployment-configurations)
- [Getting Started](#getting-started)
    + [Start spark-shell](#start-spark-shell)
    + [Get TiSpark version](#get-tispark-version)
    + [Read with TiSpark](#read-with-tispark)
    + [Write with TiSpark](#write-with-tispark)
    + [Write With JDBC DataSource](#write-with-jdbc-datasource)
- [Configuration](#configuration)
    + [Log4j Configuration](#log4j-configuration)
    + [Time Zone Configuration](#time-zone-configuration)
- [Features](#features)
    + [Expression Index](#expression-index)
    + [TiFlash](#tiflash)
    + [Partition Table support](#partition-table-support)
- [Statistics information](#statistics-information)
- [FAQ](#faq)

# Requirements

+ TiSpark supports Spark >= 2.3.x, but does not support any Spark versions earlier than 2.3.
+ TiSpark requires JDK 1.8+ and Scala 2.11/2.12.
+ TiSpark runs in any Spark mode such as `YARN`, `Mesos`, and `Standalone`.

# Recommended deployment configurations

Refer to the [Spark official website](https://spark.apache.org/docs/latest/hardware-provisioning.html) for detailed hardware recommendations.

For independent deployment of Spark cluster:
+ It is recommended to allocate 32G memory for Spark. Reserve at least 25% of the memory for the operating system and the buffer cache.
+ It is recommended to provision at least 8 to 16 cores per machine for Spark. First, you must assign all the CPU cores to Spark.

The following is an example based on the `spark-env.sh` configuration:

```
SPARK_EXECUTOR_MEMORY = 32g
SPARK_WORKER_MEMORY = 32g
SPARK_WORKER_CORES = 8
```

For the hybrid deployment of Spark and TiKV, add the resources required by Spark to the resources reserved in TiKV, and allocate 25% of the memory for the system.

# Getting Started

> Take the use of spark-shell for example, make sure you have deployed Spark and [getted the TiSpark](https://github.com/pingcap/tispark/wiki/Getting-TiSpark)

### Start spark-shell

To use TiSpark in spark-shell:

1. Add the following configuration in `spark-defaults.conf`
```
spark.sql.extensions  org.apache.spark.sql.TiExtensions
spark.tispark.pd.addresses  ${your_pd_adress}
```
2. Start spark-shell with the --jars option

```
spark-shell --jars tispark-assembly-{version}.jar
```

### Get TiSpark version

```scala
spark.sql("select ti_version()").collect
```

### Read with TiSpark
You can use Spark SQL to read from TiKV
```scala
spark.sql("select count(*) from ${database}.${table}").show
```

### Write with TiSpark

You can use Spark DataSource API to write to TiKV and guarantees ACID

```scala
val tidbOptions: Map[String, String] = Map(
  "tidb.addr" -> "127.0.0.1",
  "tidb.password" -> "",
  "tidb.port" -> "4000",
  "tidb.user" -> "root"
)

val customerDF = spark.sql("select * from customer limit 100000")

customerDF.write
.format("tidb")
.option("database", "tpch_test")
.option("table", "cust_test_select")
.options(tidbOptions)
.mode("append")
.save()
```
See [Data Source API User Guide](https://github.com/pingcap/tispark/blob/master/docs/datasource_api_userguide.md) for more details.

### Write With JDBC DataSource

You can also write to TiDB with Spark JDBC. You need not TiSpark when you decide to write in this way.

This is beyond the scope of TiSpark. We just give a simple example here, you can get detail info in [official doc](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

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

Set `isolationLevel` to `NONE` to avoid large single transactions which might lead to TiDB OOM and also avoid the `ISOLATION LEVEL does not support` error (TiDB currently only supports `REPEATABLE-READ`).

# Configuration

> The configurations in the table below can be put together with `spark-defaults.conf` or passed in the same way as other Spark configuration properties.

| Key                                             | Default Value    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|-------------------------------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `spark.tispark.pd.addresses`                    | `127.0.0.1:2379` | The addresses of PD cluster, which are split by comma                                                                                                                                                                                                                                                                                                                                                                                               |
| `spark.tispark.grpc.framesize`                  | `2147483647`     | The maximum frame size of gRPC response in bytes (default 2G)                                                                                                                                                                                                                                                                                                                                                                                       |
| `spark.tispark.grpc.timeout_in_sec`             | `10`             | The gRPC timeout time in seconds                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `spark.tispark.plan.allow_agg_pushdown`         | `true`           | Whether aggregations are allowed to push down to TiKV (in case of busy TiKV nodes)                                                                                                                                                                                                                                                                                                                                                                  |
| `spark.tispark.plan.allow_index_read`           | `true`           | Whether index is enabled in planning (which might cause heavy pressure on TiKV)                                                                                                                                                                                                                                                                                                                                                                     |
| `spark.tispark.index.scan_batch_size`           | `20000`          | The number of row key in batch for the concurrent index scan                                                                                                                                                                                                                                                                                                                                                                                        |
| `spark.tispark.index.scan_concurrency`          | `5`              | The maximal number of threads for index scan that retrieves row keys (shared among tasks inside each JVM)                                                                                                                                                                                                                                                                                                                                           |
| `spark.tispark.table.scan_concurrency`          | `512`            | The maximal number of threads for table scan (shared among tasks inside each JVM)                                                                                                                                                                                                                                                                                                                                                                   |
| `spark.tispark.request.command.priority`        | `Low`            | The value options are `Low`, `Normal`, `High`. This setting impacts the resource to get in TiKV. `Low` is recommended because the OLTP workload is not disturbed.                                                                                                                                                                                                                                                                                   |
| `spark.tispark.coprocess.codec_format`          | `chblock`        | choose the default codec format for coprocessor, available options are `default`, `chblock`, `chunk`                                                                                                                                                                                                                                                                                                                                                |
| `spark.tispark.coprocess.streaming`             | `false`          | Whether to use streaming for response fetching (experimental)                                                                                                                                                                                                                                                                                                                                                                                       |
| `spark.tispark.plan.unsupported_pushdown_exprs` | ``               | A comma-separated list of expressions. In case you have a very old version of TiKV, you might disable some of the expression push-down if they are not supported.                                                                                                                                                                                                                                                                                   |
| `spark.tispark.plan.downgrade.index_threshold`  | `1000000000`     | If the range of index scan on one Region exceeds this limit in the original request, downgrade this Region's request to table scan rather than the planned index scan. By default, the downgrade is disabled.                                                                                                                                                                                                                                       |
| `spark.tispark.show_rowid`                      | `false`          | Whether to show the implicit row ID if the ID exists                                                                                                                                                                                                                                                                                                                                                                                                |
| `spark.tispark.db_prefix`                       | ``               | The string that indicates the extra prefix for all databases in TiDB. This string distinguishes the databases in TiDB from the Hive databases with the same name.                                                                                                                                                                                                                                                                                   |
| `spark.tispark.request.isolation.level`         | `SI`             | Isolation level means whether to resolve locks for the underlying TiDB clusters. When you use the "RC", you get the latest version of record smaller than your `tso` and ignore the locks. If you use "SI", you resolve the locks and get the records depending on whether the resolved lock is committed or aborted.                                                                                                                               |
| `spark.tispark.coprocessor.chunk_batch_size`    | `1024`           | How many rows fetched from Coprocessor                                                                                                                                                                                                                                                                                                                                                                                                              |
| `spark.tispark.isolation_read_engines`          | `tikv,tiflash`   | List of readable engines of TiSpark, comma separated, storage engines not listed will not be read                                                                                                                                                                                                                                                                                                                                                   |

### Log4j Configuration

When you start `spark-shell` or `spark-sql` and run query, you might see the following warnings:
```
Failed to get database ****, returning NoSuchObjectException
Failed to get database ****, returning NoSuchObjectException
```
where `****` is the name of database.

The warnings are benign and occurs because Spark cannot find `****` in its own catalog. You can just ignore these warnings.

To mute them, append the following text to `${SPARK_HOME}/conf/log4j.properties`.

```
# tispark disable "WARN ObjectStore:568 - Failed to get database"
log4j.logger.org.apache.hadoop.hive.metastore.ObjectStore=ERROR
```

### Time Zone Configuration

Set time zone by using the `-Duser.timezone` system property (for example, `-Duser.timezone=GMT-7`), which affects the `Timestamp` type.

Do not use `spark.sql.session.timeZone`.

# Features

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

# Statistics information

TiSpark uses the statistic information for:

+ Determining which index to use in your query plan with the lowest estimated cost.
+ Small table broadcasting, which enables efficient broadcast join.

For TiSpark to use the statistic information, first make sure that relevant tables have been analyzed.

See [here](https://github.com/pingcap/docs/blob/master/statistics.md) for more details about how to analyze tables.

Since TiSpark 2.0, statistics information is default to auto-load.

# FAQ

See our wiki [TiSpark FAQ](https://github.com/pingcap/tispark/wiki/TiSpark-FAQ)
