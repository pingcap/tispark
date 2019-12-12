# TiSpark
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.pingcap.tispark/tispark-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.pingcap.tispark/tispark-core)
[![Javadocs](http://javadoc.io/badge/com.pingcap.tispark/tispark-core.svg)](http://javadoc.io/doc/com.pingcap.tispark/tispark-core)
[![codecov.io](https://codecov.io/gh/pingcap/tispark/coverage.svg?branch=master)](https://codecov.io/gh/pingcap/tispark?branch=master)
[![License](https://img.shields.io/github/license/pingcap/tispark.svg)](https://github.com/pingcap/tispark/blob/master/LICENSE)

TiSpark is a thin layer built for running Apache Spark on top of TiDB/TiKV to answer complex OLAP queries. It takes advantages of both the Spark platform and the distributed TiKV cluster while seamlessly glues to TiDB, the distributed OLTP database, to provide a Hybrid Transactional/Analytical Processing (HTAP), and serves as a one-stop solution for online transactions and analysis.

## Quick start

Read the [Quick Start](./docs/userguide.md).

## Getting TiSpark
The current stable version is **TiSpark 2.1.1** which is compatible with **Spark 2.3.0+** and **Spark 2.4.0+**.

The latest stable version compatible with **Spark 2.1.0+** is **TiSpark 1.2.1**

**When using TiSpark 1.2.1, please follow the [document for Spark 2.1](./docs/userguide_spark2.1.md)**

**When using TiSpark 2.1.4 Spark 2.3.0+, please use version `2.1.4-spark_2.3` and follow the [document for Spark 2.3+](./docs/userguide.md)**

**When using TiSpark 2.1.4 with Spark 2.4.0+, please use version `2.1.4-spark_2.4` and follow the [document for Spark 2.3+](./docs/userguide.md)**

You may also [build from sources](#how-to-build-from-sources) to try the new features on TiSpark master branch.

If you are using maven(recommended), add the following to your pom.xml:
```xml
<dependencies>
    <dependency>
      <groupId>com.pingcap.tispark</groupId>
      <artifactId>tispark-core</artifactId>
      <version>2.1.4-spark_${spark.version}</version>
    </dependency>
</dependencies>
```

For other build tools, you can visit search.maven.org and search with GroupId [![Maven Search](https://img.shields.io/badge/com.pingcap-tikv/tispark-green.svg)](http://search.maven.org/#search%7Cga%7C1%7Cpingcap)(This search will also list all available modules of TiSpark including tikv-client).

## How to build from sources
TiSpark now supports Spark 2.3.0+/2.4.0+. The previous version for Spark 2.1.0+ will only contain bug fixes in future, you may still get Spark 2.1 support until TiSpark 1.2.1.
```
git clone https://github.com/pingcap/tispark.git
```
To build all TiSpark modules from sources, please run the command under TiSpark root directory:
```
mvn clean install -Dmaven.test.skip=true -P spark-2.3
or
mvn clean install -Dmaven.test.skip=true -P spark-2.4
```
**Please note that you need to specify the major version of Spark according to the Spark version you are using.**

Remember to add `-Dmaven.test.skip=true` to skip all the tests if you don't need to run them.

## How to choose TiSpark Version

| Spark Version | Stable TiSpark Version |
| ------------- | ---------------------- |
| Spark-2.4.x | TiSpark-2.1.4 |
| Spark-2.3.x | TiSpark-2.1.4 |
| Spark-2.2.x | TiSpark-1.2.1 |
| Spark-2.1.x | TiSpark-1.2.1 |

## Maximum TiDB/TiKV/PD version supported by TiSpark

Each latest TiSpark version guarantees *backward compatibility* for TiDB components, i.e., supports TiDB/TiKV/PD until a certain release. Its reason varies, amongst which the most common one is that the new features and bug-fixes provided by TiDB components require an update on API, dependencies, etc.

| TiSpark Version | Maximum TiDB Version | Maximum TiKV Version | Maximum PD Version |
| ----- | ------ | ------ | ------ |
| < 1.2 | v2.1.4 | v2.1.4 | v2.1.4 |
| 1.2.x | v2.1.x | v2.1.x | v2.1.x |
| 2.x | v3.0.0-beta | v3.0.0-beta | v3.0.0-beta |
| Latest (master) | Latest | Latest | Latest |

## Available Spark version supported by TiSpark

While TiSpark provides downward compatibility for TiDB, it guarantees **restricted** Spark version support for means of catching up to the latest Datasource API changes.

| TiSpark Version | Spark Version |
| ----- | ------ |
| 1.x | Spark v2.1.0+ |
| 2.0 | Spark v2.3.0+ |
| 2.1.x | Spark v2.3.0+, Spark v2.4.0+ |
| Latest (master) | Spark v2.3.0+, Spark v2.4.0+ |

## How to migrate from Spark 2.1 to Spark 2.3/2.4
For users using Spark 2.1 who wish to migrate to latest TiSpark on Spark 2.3/2.4, please download or install Spark 2.3+/2.4+ following instructions on [Apache Spark Site](http://spark.apache.org/downloads.html) and overwrite the old spark version in `$SPARK_HOME`.

## Scala Version
TiSpark currently only supports `scala-2.11`.

## TiSpark Architecture

![architecture](./docs/architecture.png)


- TiSpark integrates with Spark Catalyst Engine deeply. It provides precise control of computing, which allows Spark to read data from TiKV efficiently. It also supports index seek, which improves the performance of the point query execution significantly.

- It utilizes several strategies to push down the computing to reduce the size of dataset handling by Spark SQL, which accelerates the query execution. It also uses the TiDB built-in statistical information for  the query plan optimization.

- From the data integration point of view, TiSpark + TiDB provides a solution runs both transaction and analysis directly on the same platform without building and maintaining any ETLs. It simplifies the system architecture and reduces the cost of maintenance.

- In addition, you can deploy and utilize tools from the Spark ecosystem for further data processing and manipulation on TiDB. For example, using TiSpark for data analysis and ETL; retrieving data from TiKV as a machine learning data source; generating reports from the scheduling system  and so on.

TiSpark depends on the existence of TiKV clusters and PDs. It also needs to set up and use Spark clustering platform.

A thin layer of TiSpark. Most of the logic is inside tikv-client library.
https://github.com/pingcap/tispark/tree/master/tikv-client


## Quick Start
**Before everything starts, you must add `spark.sql.extensions  org.apache.spark.sql.TiExtensions` in spark-defaults.conf**
**You should also confirm that `spark.tispark.pd.addresses` is set correctly**

From Spark-shell:
```
./bin/spark-shell --jars /wherever-it-is/tispark-${name_with_version}.jar
```
For TiSpark version >= 2.0:
```
spark.sql("use tpch_test")

spark.sql("select count(*) from lineitem").show
```
For TiSpark version < 2.0:
```
import org.apache.spark.sql.TiContext
val ti = new TiContext (spark)
ti.tidbMapDatabase ("tpch_test")

spark.sql("select count(*) from lineitem").show
```

**Please Note: For now even if you use TiSpark 2.0+, for spark-submit on pyspark, tidbMapDatabase is still required and TiExtension is not supported yet. We are working on it.

## Current Version
```
spark.sql("select ti_version()").show
```

## TiDB Data Source API
When using the TiDB Data Source API, please follow the document for [TiDB Data Source API User Guide](./docs/datasource_api_userguide.md).

## Configuration

Below configurations can be put together with spark-defaults.conf or passed in the same way as other Spark config properties.

|    Key    | Default Value | Description |
| ---------- | --- | --- |
| spark.tispark.pd.addresses |  127.0.0.1:2379 | PD Cluster Addresses, split by comma |
| spark.tispark.grpc.framesize |  268435456 | Max frame size of GRPC response |
| spark.tispark.grpc.timeout_in_sec |  10 | GRPC timeout time in seconds |
| spark.tispark.plan.allow_agg_pushdown |  true | If allow aggregation pushdown (in case of busy TiKV nodes) |
| spark.tispark.plan.allow_index_read |  true | If allow index read (which might cause heavy pressure on TiKV) |
| spark.tispark.index.scan_batch_size |  20000 | How many row key in batch for concurrent index scan |
| spark.tispark.index.scan_concurrency |  5 | Maximal threads for index scan retrieving row keys (shared among tasks inside each JVM) |
| spark.tispark.table.scan_concurrency |  512 | Maximal threads for table scan (shared among tasks inside each JVM) |
| spark.tispark.request.command.priority |  "Low" | "Low", "Normal", "High" which impacts resource to get in TiKV. Low is recommended for not disturbing OLTP workload |
| spark.tispark.coprocess.streaming |  false | Whether to use streaming for response fetching (Experimental) |
| spark.tispark.plan.unsupported_pushdown_exprs |  "" | A comma-separated list of expressions. In case you have a very old version of TiKV, you might disable some of the expression push-down if not supported |
| spark.tispark.plan.downgrade.index_threshold | 1000000000 | If index scan ranges on one region exceed this limit in the original request, downgrade this region's request to table scan rather than original planned index scan, by default the downgrade is turned off |
| spark.tispark.show_rowid |  false | If to show implicit row Id if exists |
| spark.tispark.db_prefix |  "" | A string indicating the extra database prefix for all databases in TiDB to distinguish them from Hive databases with the same name |
| spark.tispark.request.isolation.level |  "SI" | Isolation level means whether do the resolve lock for the underlying tidb clusters. When you use the "RC", you will get the newest version of record smaller than your tso and ignore the locks. And if you use "SI", you will resolve the locks and get the records according to whether the resolved lock is committed or aborted  |

## Log4j Configuration
When you start `spark-shell` or `spark-sql` and run query, you might see the following warnings:
```
Failed to get database ****, returning NoSuchObjectException
Failed to get database ****, returning NoSuchObjectException
```
where `****` is the name of database.

This is due to spark cannot find `****` in its own catalog. The two warning messages are benign, you can just ignore them.

If you want to get rid of them, you can append the following text to `${SPARK_HOME}/conf/log4j.properties`.
```
# tispark disable "WARN ObjectStore:568 - Failed to get database"
log4j.logger.org.apache.hadoop.hive.metastore.ObjectStore=ERROR
```

## Time Zone
Time Zone can be set by using `-Duser.timezone` system property, e.g. `-Duser.timezone=GMT-7`, which will affect `Timestamp` type.  Please do not use `spark.sql.session.timeZone`.

## Statistics information
If you want to know how TiSpark could benefit from TiDB's statistic information, read more [here](./docs/userguide.md).

## Compatibility with tidb-3.0
### View
TiDB starts to support `view` from `tidb-3.0`.

TiSpark currently **does not support** `view`. Users will not be able to observe or access data through views with TiSpark.

### Table Partition
`tidb-3.0` supports both `Range Partition` and `Hash Partition`.

TiSpark currently **supports** `Range Partition` and `Hash Partition`. Users can select data from `Range Partition` table and `Hash Partition` table through TiSpark.

In most cases, TiSpark will use a full table scan. Only in some cases, TiSpark will apply partition pruning (read more [here](./docs/userguide.md).

## Example Programs
There are some [sample programs](https://github.com/pingcap/tispark-test/tree/master/tispark-examples) for TiSpark. You can run them locally or on a cluster following the document.

## How to test
We use [docker-compose](https://docs.docker.com/compose/) to provide tidb cluster service which allows you to run test across different platforms. It is recommended to install docker in order to test locally, or you can set up your own TiDB cluster locally as you wish.

If you prefer the docker way, you can use `docker-compose up -d` to launch tidb cluster service under tispark home directory. If you want to see tidb cluster's log you can launch via `docker-compose up`. You can use `docker-compose down` to shutdown entire tidb cluster service. All data is stored in the `data` directory at the root of this project. Feel free to change it.

You can read more about the test [here](./core/src/test/Readme.md).

## Follow us

### Twitter

[@PingCAP](https://twitter.com/PingCAP)

### Mailing list

tidb-user@googlegroups.com

[Google Group](https://groups.google.com/forum/#!forum/tidb-user)

## License
TiSpark is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
