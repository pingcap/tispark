# TiSpark (version >= 3.0) User Guide

> **Note:**
>
> This is a user guide for TiSpark version >= 3.0 which is compatible with spark 3.x. If you are using Spark version earlier than 3.0, refer to [Document for TiSpark 2.4](./userguide_2.0.md).
> 
> TiSpark >= 3.0 is developed with Spark catalog plugin. If you want to use TiSpark without catalog plugin mode, please turn to TiSpark 2.5 (compatible with Spark 3.0 and 3.1)

- [Requirements](#requirements)
- [Recommended deployment configurations of Spark](#recommended-deployment-configurations-of-spark)
- [Getting TiSpark](#getting-tispark)
  + [Get mysql-connector-j](#get-mysql-connector-j)
  + [Choose TiSpark Version](#choose-tispark-version)
  + [Get TiSpark jar](#get-tispark-jar)
  + [TiSpark jar's Artifact ID](#tispark-jar-s-artifact-id)
- [Getting Started](#getting-started)
  + [Start spark-shell](#start-spark-shell)
  + [Get TiSpark version](#get-tispark-version)
  + [Read with TiSpark](#read-with-tispark)
  + [Write with TiSpark](#write-with-tispark)
  + [Write With JDBC DataSource](#write-with-jdbc-datasource)
  + [Delete with TiSpark](#delete-with-tispark)
  + [Use with other data source](#use-with-other-data-source)
- [Configuration](#configuration)
  + [TLS Configuration](#tls-configuration)
  + [Log4j Configuration](#log4j-configuration)
  + [Time Zone Configuration](#time-zone-configuration)
- [Features](#features)
  + [Expression Index](#expression-index)
  + [TiFlash](#tiflash)
  + [Partition Table support](#partition-table-support)
  + [Other Features](#other-features)
- [Statistics information](#statistics-information)
- [FAQ](#faq)

# Requirements

+ TiSpark supports Spark >= 2.3, but does not support any Spark versions earlier than 2.3.
+ TiSpark requires JDK 1.8 and Scala 2.11/2.12.
+ TiSpark runs in any Spark mode such as `YARN`, `Mesos`, and `Standalone`.

# Recommended deployment configurations of Spark

Since TiSpark is a TiDB connector of Spark, to use it, a running Spark cluster is required.

Here we give some basic advice for the deployment of Spark. Please Turn to the [Spark official website](https://spark.apache.org/docs/latest/hardware-provisioning.html) for detailed hardware recommendations.

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

# Getting TiSpark

TiSpark is a third-party jar package for Spark that provides the ability to read/write TiKV

### Get mysql-connector-j

We will not provide the `mysql-connector-java` dependency because of the limit of the GPL license.

The following versions of TiSpark's jar will no longer include `mysql-connector-java`.
- TiSpark > 3.0.1
- TiSpark > 2.5.1 for TiSpark 2.5.x
- TiSpark > 2.4.3 for TiSpark 2.4.x

Now, TiSpark needs `mysql-connector-java` for writing and auth. Please import `mysql-connector-java` manually when you need to write or auth.

- you can import it by putting the jar into spark jars file

- you can also import it when you submit spark job like
```
spark-submit --jars tispark-assembly-3.0_2.12-3.1.0-SNAPSHOT.jar,mysql-connector-java-8.0.29.jar
```

### Choose TiSpark Version

You can choose TiSpark version according to your TiDB and Spark version

| TiSpark version | TiDB、TiKV、PD version | Spark version | Scala version |
| ---------------  | -------------------- | ------------- | ------------- |
| 2.4.x-scala_2.11 | 5.x, 4.x             | 2.3.x, 2.4.x   | 2.11          |
| 2.4.x-scala_2.12 | 5.x, 4.x             | 2.4.x         | 2.12          |
| 2.5.x            | 5.x, 4.x             | 3.0.x, 3.1.x   | 2.12          |
| 3.0.x            | 5.x, 4.x             | 3.0.x, 3.1.x, 3.2.x|2.12|
| 3.1.x            | 6.x, 5.x, 4.x             | 3.0.x, 3.1.x, 3.2.x, 3.3.x|2.12|
- TiSpark 2.4.3, 2.5.2, 3.0.2,3.1.0 is the latest stable version, which is highly recommended.

### Get TiSpark jar
- get from [maven central](https://search.maven.org/) and search with GroupId [![Maven Search](https://img.shields.io/badge/com.pingcap-tikv/tispark-green.svg)](http://search.maven.org/#search%7Cga%7C1%7Cpingcap)
- get from [TiSpark releases](https://github.com/pingcap/tispark/releases)
- build from source with the steps below

Currently, java8 is the only choice to build TiSpark, run mvn -version to check.
```
git clone https://github.com/pingcap/tispark.git
```
Run the following command under the TiSpark root directory:

```
// add -Dmaven.test.skip=true to skip the tests
mvn clean install -Dmaven.test.skip=true
// or you can add properties to specify spark version
mvn clean install -Dmaven.test.skip=true -Pspark3.2.1
```

### TiSpark jar's Artifact ID
> The Artifact ID of TiSpark is a bit different in different TiSpark version

| TiSpark version               | Artifact ID                                        |
|-------------------------------| -------------------------------------------------- |
| 2.4.x-${scala_version}, 2.5.0 | tispark-assembly                                   |
| 2.5.1                         | tispark-assembly-${spark_version}                  |
| 3.0.x, 3.1.x                  | tispark-assembly-${spark_version}_${scala_version} |

# Getting Started

> Take the use of spark-shell for example, make sure you have deployed Spark.

### Start spark-shell

To use TiSpark in spark-shell:

1. Add the following configuration in `spark-defaults.conf`
```
spark.sql.extensions  org.apache.spark.sql.TiExtensions
spark.tispark.pd.addresses  ${your_pd_adress}
spark.sql.catalog.tidb_catalog  org.apache.spark.sql.catalyst.catalog.TiCatalog
spark.sql.catalog.tidb_catalog.pd.addresses  ${your_pd_adress}
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
spark.sql("use tidb_catalog")
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

You can also write with Spark SQL since TiSpark 3.1. See [insert SQL](features/insert_sql_userguide.md) for more detail.

### Write With JDBC DataSource

You can also write to TiDB with Spark JDBC. You need not TiSpark when you decide to write in this way.

This is beyond the scope of TiSpark. We just give a simple example here, you can get detailed info in [official doc](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

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

### Delete with TiSpark
You can use Spark SQL to delete from TiKV (Tispark master support)

```
spark.sql("use tidb_catalog")
spark.sql("delete from ${database}.${table} where xxx")
```
See [here](https://github.com/pingcap/tispark/blob/master/docs/delete_userguide.md) for more details.

### Use with other data source
> you can use multiple catalogs to read from different data sources.
```
// read from hive
spark.sql("select * from spark_catalog.default.t").show

// join hive and tidb
spark.sql("select t1.id,t2.id from spark_catalog.default.t t1 left join tidb_catalog.test.t t2").show
```

# Configuration

> The configurations in the table below can be put together with `spark-defaults.conf` or passed in the same way as other Spark configuration properties.

<<<<<<< HEAD
| Key                                            | Default Value    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|------------------------------------------------|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `spark.tispark.pd.addresses`                   | `127.0.0.1:2379` | The addresses of PD cluster, which are split by comma                                                                                                                                                                                                                                                                                                                                                                                             |
| `spark.tispark.grpc.framesize`                 | `2147483647`     | The maximum frame size of gRPC response in bytes (default 2G)                                                                                                                                                                                                                                                                                                                                                                                     |
| `spark.tispark.grpc.timeout_in_sec`            | `180`              | The gRPC timeout time in seconds.                                                                                                                                                                                                                                                                                                                                                                                  |
| `spark.tispark.plan.allow_agg_pushdown`        | `true`           | Whether aggregations are allowed to push down to TiKV (in case of busy TiKV nodes)                                                                                                                                                                                                                                                                                                                                                                |
| `spark.tispark.plan.allow_index_read`          | `true`           | Whether index is enabled in planning (which might cause heavy pressure on TiKV)                                                                                                                                                                                                                                                                                                                                                                   |
| `spark.tispark.index.scan_batch_size`          | `20000`          | The number of row key in batch for the concurrent index scan                                                                                                                                                                                                                                                                                                                                                                                      |
| `spark.tispark.index.scan_concurrency`         | `5`              | The maximal number of threads for index scan that retrieves row keys (shared among tasks inside each JVM)                                                                                                                                                                                                                                                                                                                                         |
| `spark.tispark.table.scan_concurrency`         | `512`            | The maximal number of threads for table scan (shared among tasks inside each JVM)                                                                                                                                                                                                                                                                                                                                                                 |
| `spark.tispark.request.command.priority`       | `Low`            | The value options are `Low`, `Normal`, `High`. This setting impacts the resource to get in TiKV. `Low` is recommended because the OLTP workload is not disturbed.                                                                                                                                                                                                                                                                                 |
| `spark.tispark.coprocess.codec_format`         | `chblock`        | choose the default codec format for coprocessor, available options are `default`, `chblock`, `chunk`                                                                                                                                                                                                                                                                                                                                              |
| `spark.tispark.coprocess.streaming`            | `false`          | Whether to use streaming for response fetching (experimental)                                                                                                                                                                                                                                                                                                                                                                                     |
| `spark.tispark.plan.unsupported_pushdown_exprs` | ``               | A comma-separated list of expressions. In case you have a very old version of TiKV, you might disable some of the expression push-down if they are not supported.                                                                                                                                                                                                                                                                                 |
| `spark.tispark.plan.downgrade.index_threshold` | `1000000000`     | If the range of index scan on one Region exceeds this limit in the original request, downgrade this Region's request to table scan rather than the planned index scan. By default, the downgrade is disabled.                                                                                                                                                                                                                                     |
| `spark.tispark.show_rowid`                     | `false`          | Whether to show the implicit row ID if the ID exists                                                                                                                                                                                                                                                                                                                                                                                              |
| `spark.tispark.db_prefix`                      | ``               | The string that indicates the extra prefix for all databases in TiDB. This string distinguishes the databases in TiDB from the Hive databases with the same name.                                                                                                                                                                                                                                                                                 |
| `spark.tispark.request.isolation.level`        | `SI`             | Isolation level means whether to resolve locks for the underlying TiDB clusters. When you use the "RC", you get the latest version of record smaller than your `tso` and ignore the locks. If you use "SI", you resolve the locks and get the records depending on whether the resolved lock is committed or aborted.                                                                                                                             |
| `spark.tispark.coprocessor.chunk_batch_size`   | `1024`           | How many rows fetched from Coprocessor                                                                                                                                                                                                                                                                                                                                                                                                            |
| `spark.tispark.isolation_read_engines`         | `tikv,tiflash`   | List of readable engines of TiSpark, comma separated, storage engines not listed will not be read                                                                                                                                                                                                                                                                                                                                                 |
| `spark.tispark.stale_read`                     | it is optional   | The stale read timestamp(ms). see [here](features/stale_read.md) for more detail                                                                                                                                                                                                                                                                                                                                                                  |
| `spark.tispark.tikv.tls_enable`                | `false`          | Whether to enable TiSpark TLS. 　                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `spark.tispark.tikv.trust_cert_collection`     | ``               | Trusted certificates for TiKV Client, which is used for verifying the remote pd's certificate, e.g. `/home/tispark/config/root.pem` The file should contain an X.509 certificate collection.                                                                                                                                                                                                                                                      |
| `spark.tispark.tikv.key_cert_chain`            | ``               | An X.509 certificate chain file for TiKV Client, e.g. `/home/tispark/config/client.pem`.                                                                                                                                                                                                                                                                                                                                                          |
| `spark.tispark.tikv.key_file`                  | ``               | A PKCS#8 private key file for TiKV Client, e.g. `/home/tispark/client_pkcs8.key`.                                                                                                                                                                                                                                                                                                                                                                 |
| `spark.tispark.tikv.jks_enable`                | `false`          | Whether to use the JAVA key store instead of the X.509 certificate.                                                                                                                                                                                                                                                                                                                                                                               |
| `spark.tispark.tikv.jks_trust_path`            | ``               | A JKS format certificate for TiKV Client, that is generated by `keytool`, e.g. `/home/tispark/config/tikv-truststore`.                                                                                                                                                                                                                                                                                                                            |
| `spark.tispark.tikv.jks_trust_password`        | ``               | The password of `spark.tispark.tikv.jks_trust_path`.                                                                                                                                                                                                                                                                                                                                                                                              |
| `spark.tispark.tikv.jks_key_path`              | ``               | A JKS format key for TiKV Client generated by `keytool`, e.g. `/home/tispark/config/tikv-clientstore`.                                                                                                                                                                                                                                                                                                                                            |
| `spark.tispark.tikv.jks_key_password`          | ``               | The password of `spark.tispark.tikv.jks_key_path`.                                                                                                                                                                                                                                                                                                                                                                                                |
| `spark.tispark.jdbc.tls_enable`                | `false`          | Whether to enable TLS when using JDBC connector.                                                                                                                                                                                                                                                                                                                                                                                                  |
| `spark.tispark.jdbc.server_cert_store`         | ``               | Trusted certificates for JDBC. This is a JKS format certificate generated by `keytool`, e.g. `/home/tispark/config/jdbc-truststore`. Default is "", which means TiSpark doesn't verify TiDB server.                                                                                                                                                                                                                                               |
| `spark.tispark.jdbc.server_cert_password`      | ``               | The password of `spark.tispark.jdbc.server_cert_store`.                                                                                                                                                                                                                                                                                                                                                                                           |
| `spark.tispark.jdbc.client_cert_store`         | ``               | A PKCS#12 certificate for JDBC. It is a JKS format certificate generated by `keytool`, e.g. `/home/tispark/config/jdbc-clientstore`. Default is "", which means TiDB server doesn't verify TiSpark.                                                                                                                                                                                                                                               |
| `spark.tispark.jdbc.client_cert_password`      | ``               | The password of `spark.tispark.jdbc.client_cert_store`.                                                                                                                                                                                                                                                                                                                                                                                           |
| `spark.tispark.tikv.tls_reload_interval`       | `10s`            | The interval time of checking if there is any reloading certificates. The default is `10s` (10 seconds).                                                                                                                                                                                                                                                                                                                                          |
| `spark.tispark.tikv.conn_recycle_time`         | `60s`            | The interval time of cleaning the expired connection with TiKV. Only take effect when enabling cert reloading. The default is `60s` (60 seconds).                                                                                                                                                                                                                                                                                                 |
| `spark.tispark.host_mapping`                   | ``               | This is route map used to configure public IP and intranet IP mapping. When the TiDB cluster is running on the intranet, you can map a set of intranet IPs to public IPs for an outside Spark cluster to access. The format is `{Intranet IP1}:{Public IP1};{Intranet IP2}:{Public IP2}`, e.g. `192.168.0.2:8.8.8.8;192.168.0.3:9.9.9.9`.                                                                                                         |
| `spark.tispark.new_collation_enable`           | ``               | When TiDB cluster enable [new collation](https://docs.pingcap.com/tidb/stable/character-set-and-collation#new-framework-for-collations), this configuration should be `true`, otherwise it should be `false`. If this item is not configured, TiSpark will configure automatically based on the TiDB version. The configuration rule is as follows: If the TiDB version is greater than or equal to v6.0.0, it is `true`; otherwise, it is `false`. |
| `spark.tispark.replica_read`                   | leader           | Read data from specified role. The optional roles are leader, follower and learner. You can also specify multiple roles, and we will pick the roles you specify in order. See [here](.features/follower_read.md)for more detail                                                                                                                                                                                                                   |
| `spark.tispark.replica_read.label`             | ""               | Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y                                                                                                                                                                                                                                                                                                                                                            |
| `spark.tispark.replica_read.address_whitelist` | ""               | Only select TiKV store with given ip addresses. Split mutil addresses by `,`                                                                                                                                                                                                                                                                                                                                                                      |
| `spark.tispark.replica_read.address_blacklist` | ""               | Do not select TiKV store with given ip addresses. Split mutil addresses by `,`                                                                                                                                                                                                                                                                                                                                                                    |
=======
| Key                                             | Default Value    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|-------------------------------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `spark.tispark.pd.addresses`                    | `127.0.0.1:2379` | The addresses of PD cluster, which are split by comma                                                                                                                                                                                                                                                                                                                                                                                               |
| `spark.tispark.grpc.framesize`                  | `2147483647`     | The maximum frame size of gRPC response in bytes (default 2G)                                                                                                                                                                                                                                                                                                                                                                                       |
| `spark.tispark.grpc.timeout_in_sec`             | `180`            | The gRPC timeout time in seconds                                                                                                                                                                                                                                                                                                                                                                                                                    |
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
| `spark.tispark.stale_read`                      | it is optional   | The stale read timestamp(ms). see [here](features/stale_read.md) for more detail                                                                                                                                                                                                                                                                                                                                                                    |
| `spark.tispark.tikv.tls_enable`                 | `false`          | Whether to enable TiSpark TLS. 　                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `spark.tispark.tikv.trust_cert_collection`      | ``               | Trusted certificates for TiKV Client, which is used for verifying the remote pd's certificate, e.g. `/home/tispark/config/root.pem` The file should contain an X.509 certificate collection.                                                                                                                                                                                                                                                        |
| `spark.tispark.tikv.key_cert_chain`             | ``               | An X.509 certificate chain file for TiKV Client, e.g. `/home/tispark/config/client.pem`.                                                                                                                                                                                                                                                                                                                                                            |
| `spark.tispark.tikv.key_file`                   | ``               | A PKCS#8 private key file for TiKV Client, e.g. `/home/tispark/client_pkcs8.key`.                                                                                                                                                                                                                                                                                                                                                                   |
| `spark.tispark.tikv.jks_enable`                 | `false`          | Whether to use the JAVA key store instead of the X.509 certificate.                                                                                                                                                                                                                                                                                                                                                                                 |
| `spark.tispark.tikv.jks_trust_path`             | ``               | A JKS format certificate for TiKV Client, that is generated by `keytool`, e.g. `/home/tispark/config/tikv-truststore`.                                                                                                                                                                                                                                                                                                                              |
| `spark.tispark.tikv.jks_trust_password`         | ``               | The password of `spark.tispark.tikv.jks_trust_path`.                                                                                                                                                                                                                                                                                                                                                                                                |
| `spark.tispark.tikv.jks_key_path`               | ``               | A JKS format key for TiKV Client generated by `keytool`, e.g. `/home/tispark/config/tikv-clientstore`.                                                                                                                                                                                                                                                                                                                                              |
| `spark.tispark.tikv.jks_key_password`           | ``               | The password of `spark.tispark.tikv.jks_key_path`.                                                                                                                                                                                                                                                                                                                                                                                                  |
| `spark.tispark.jdbc.tls_enable`                 | `false`          | Whether to enable TLS when using JDBC connector.                                                                                                                                                                                                                                                                                                                                                                                                    |
| `spark.tispark.jdbc.server_cert_store`          | ``               | Trusted certificates for JDBC. This is a JKS format certificate generated by `keytool`, e.g. `/home/tispark/config/jdbc-truststore`. Default is "", which means TiSpark doesn't verify TiDB server.                                                                                                                                                                                                                                                 |
| `spark.tispark.jdbc.server_cert_password`       | ``               | The password of `spark.tispark.jdbc.server_cert_store`.                                                                                                                                                                                                                                                                                                                                                                                             |
| `spark.tispark.jdbc.client_cert_store`          | ``               | A PKCS#12 certificate for JDBC. It is a JKS format certificate generated by `keytool`, e.g. `/home/tispark/config/jdbc-clientstore`. Default is "", which means TiDB server doesn't verify TiSpark.                                                                                                                                                                                                                                                 |
| `spark.tispark.jdbc.client_cert_password`       | ``               | The password of `spark.tispark.jdbc.client_cert_store`.                                                                                                                                                                                                                                                                                                                                                                                             |
| `spark.tispark.tikv.tls_reload_interval`        | `10s`            | The interval time of checking if there is any reloading certificates. The default is `10s` (10 seconds). `0s` (0 seconds) means disable certificates reload.                                                                                                                                                                                                                                                                                        |
| `spark.tispark.tikv.conn_recycle_time`          | `60s`            | The interval time of cleaning the expired connection with TiKV. Only take effect when enabling reloading certificates. The default is `60s` (60 seconds).                                                                                                                                                                                                                                                                                           |
| `spark.tispark.host_mapping`                    | ``               | This is route map used to configure public IP and intranet IP mapping. When the TiDB cluster is running on the intranet, you can map a set of intranet IPs to public IPs for an outside Spark cluster to access. The format is `{Intranet IP1}:{Public IP1};{Intranet IP2}:{Public IP2}`, e.g. `192.168.0.2:8.8.8.8;192.168.0.3:9.9.9.9`.                                                                                                           |
| `spark.tispark.new_collation_enable`            | ``               | When TiDB cluster enable [new collation](https://docs.pingcap.com/tidb/stable/character-set-and-collation#new-framework-for-collations), this configuration should be `true`, otherwise it should be `false`. If this item is not configured, TiSpark will configure automatically based on the TiDB version. The configuration rule is as follows: If the TiDB version is greater than or equal to v6.0.0, it is `true`; otherwise, it is `false`. |
| `spark.tispark.replica_read`                    | leader           | Read data from specified role. The optional roles are leader, follower and learner. You can also specify multiple roles, and we will pick the roles you specify in order. See [here](.features/follower_read.md)for more detail                                                                                                                                                                                                                     |
| `spark.tispark.replica_read.label`              | ""               | Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y                                                                                                                                                                                                                                                                                                                                                              |
| `spark.tispark.replica_read.address_whitelist`  | ""               | Only select TiKV store with given ip addresses. Split mutil addresses by `,`                                                                                                                                                                                                                                                                                                                                                                        |
| `spark.tispark.replica_read.address_blacklist`  | ""               | Do not select TiKV store with given ip addresses. Split mutil addresses by `,`                                                                                                                                                                                                                                                                                                                                                                      |
| `spark.tispark.enable_grpc_forward`             | false            | Whether enable grpc forward. Set it to true to enable high available between TiKV.                                                                                                                                                                                                                                                                                                                                                                  |
| `spark.tispark.gc_max_wait_time`                | 86400            | The maximum time in seconds that TiSpark block the GC safe point                                                                                                                                                                                                                                                                                                                                                                                    |
| `spark.tispark.load_tables`                     | true             | (experimental) Whether load all tables when we reload catalog cache. Disable it may cause table not find in scenarios where the table changes frequently.                                                                                                                                                                                                                                                                                           |
>>>>>>> e0c15f66c (Do not reload table schema when update catalog cache (#2667))

### TLS Configuration

TiSpark TLS has two parts: TiKV Client TLS and JDBC connector TLS. When you want to enable TLS in TiSpark, you need to configure two parts of configuration.
`spark.tispark.tikv.xxx` is used for TiKV Client to create TLS connection with PD and TiKV server.
While `spark.tispark.jdbc.xxx` is used for JDBC connect with TiDB server in TLS connection.

When TiSpark TLS is enabled, either the X.509 certificate with `tikv.trust_cert_collection`, `tikv.key_cert_chain` and `tikv.key_file` configurations or the JKS certificate with `tikv.jks_enable`, `tikv.jks_trust_path` and `tikv.jks_key_path` must be configured.
While `jdbc.server_cert_store` and `jdbc.client_cert_store` is optional.

TiSpark only supports TLSv1.2 and TLSv1.3 version.

* An example of opening TLS configuration with the X.509 certificate in TiKV Client.

```
spark.tispark.tikv.tls_enable                                  true
spark.tispark.tikv.trust_cert_collection                       /home/tispark/root.pem
spark.tispark.tikv.key_cert_chain                              /home/tispark/client.pem
spark.tispark.tikv.key_file                                    /home/tispark/client.key
```

* An example for enabling TLS with JKS configurations in TiKV Client.

```
spark.tispark.tikv.tls_enable                                  true
spark.tispark.tikv.jks_enable                                  true
spark.tispark.tikv.jks_key_path                                /home/tispark/config/tikv-truststore
spark.tispark.tikv.jks_key_password                            tikv_trustore_password
spark.tispark.tikv.jks_trust_path                              /home/tispark/config/tikv-clientstore
spark.tispark.tikv.jks_trust_password                          tikv_clientstore_password
```

When JKS and X.509 cert are set simultaneously, JKS would have a higher priority.
That means TLS builder will use JKS cert first. So, do not set `spark.tispark.tikv.jks_enable=true` when you just want to use a common PEM cert.

* An example for enabling TLS in JDBC connector.

```
spark.tispark.jdbc.tls_enable                                  true
spark.tispark.jdbc.server_cert_store                           /home/tispark/jdbc-truststore
spark.tispark.jdbc.server_cert_password                        jdbc_truststore_password
spark.tispark.jdbc.client_cert_store                           /home/tispark/jdbc-clientstore
spark.tispark.jdbc.client_cert_password                        jdbc_clientstore_password
```

For how to open TiDB TLS, see [here](https://docs.pingcap.com/tidb/dev/enable-tls-between-clients-and-servers).
For how to generate a JAVA key store, see [here](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-using-ssl.html).

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

| Feature Support                 | TiSpark 2.4.x | TiSpark 2.5.x | TiSpark 3.0.x | TiSpark 3.1.x |
|---------------------------------| ------------- | ------------- | ----------- |---------------|
| SQL select without tidb_catalog | ✔           | ✔           |             |               |
| SQL select with tidb_catalog    |               | ✔           | ✔         | ✔             |
| DataFrame append                | ✔           | ✔           | ✔         | ✔             |
| DataFrame reads                 | ✔           | ✔           | ✔         | ✔             |
| SQL show databases              | ✔           | ✔           | ✔         | ✔             |
| SQL show tables                 | ✔           | ✔           | ✔         | ✔             |
| SQL auth                        |               | ✔           | ✔         | ✔             |
| SQL delete                      |               |               | ✔         | ✔             |
| SQL insert                      |               |               |           | ✔              |
| TLS                             |               |               | ✔         | ✔             |
| DataFrame auth                  |               |               |             | ✔             |

### Expression Index

`tidb-5.0` supports Expression Index.

TiSpark currently supports retrieving data from table with `Expression Index`, but the `Expression Index` will not be used by the planner of TiSpark.

### TiFlash

TiSpark can read from TiFlash with the configuration `spark.tispark.isolation_read_engines`

### Partition Table support
 
**Reading partition table from TiDB**

TiSpark reads the range and hash partition table from TiDB.

Currently, TiSpark doesn't support a MySQL/TiDB partition table syntax `select col_name from table_name partition(partition_name)`, but you can still use `where` condition to filter the partitions.

TiSpark decides whether to apply partition pruning according to the partition type and the partition expression associated with the table. Currently, TiSpark partially apply partition pruning on range partition.

The partition pruning is applied when the partition expression of the range partition is one of the following:

+ column expression
+ `YEAR(col)` and its type is datetime/string/date literal that can be parsed as datetime.
+ `TO_DAYS(col)` and its type is datetime/string/date literal that can be parsed as datetime.

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
> Because different character sets and collations have different sort orders, the character sets and 
> collations in use may affect which partition of a table partitioned by RANGE COLUMNS a given row 
> is stored in when using string columns as partitioning columns.
> For supported character sets and collations, see [Limitations](../README.md#limitations)

### Other Features
- [Push down](features/push_down.md)
- [Delete with TiSpark](features/delete_userguide.md)
- [Stale read](features/stale_read.md)
- [Authorization and authentication](features/authorization_userguide.md)
- [TiSpark with multiple catalogs](https://github.com/pingcap/tispark/wiki/TiSpark-with-multiple-catalogs)
- TiSpark TLS : See TLS Configuration section in this article
- [TiSpark Telemetry](features/telemetry.md)
- [TiSpark plan](features/query_execution_plan_in_TiSpark.md)

# Statistics information

TiSpark uses the statistic information for:

+ Determining which index to use in your query plan with the lowest estimated cost.
+ Small table broadcasting, which enables efficient broadcast join.

For TiSpark to use the statistic information, first make sure that relevant tables have been analyzed.

See [here](https://github.com/pingcap/docs/blob/master/statistics.md) for more details about how to analyze tables.

Since TiSpark 2.0, statistics information is default to auto-load.

> **Note:**
>
> Table statistics is cached in your Spark driver node's memory, so you need to make sure that the memory is large enough for the statistics information.
Currently, you can adjust these configurations in your `spark.conf` file.

| Property Name | Default | Description
| --------   | -----:   | :----: |
| `spark.tispark.statistics.auto_load` | `true` | Whether to load the statistics information automatically during database mapping |

# FAQ

See our wiki [TiSpark FAQ](https://github.com/pingcap/tispark/wiki/TiSpark-FAQ)
