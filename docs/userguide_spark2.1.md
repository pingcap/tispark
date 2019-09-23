# TiSpark (with version < 2.0) User Guide

> **Note:**
>
> This is a user guide for TiSpark version < 2.0. If you are using version >= 2.0, refer to [Document for Spark 2.3](./userguide.md)

This document introduces how to set up and use TiSpark, which requires some basic knowledge of Apache Spark. Refer to [Spark website](https://spark.apache.org/docs/latest/index.html) for details.

## Overview

TiSpark is a thin layer built for running Apache Spark on top of TiDB/TiKV to answer the complex OLAP queries. While enjoying the merits of both the Spark platform and the distributed clusters of TiKV, it is seamlessly integrated with TiDB, the distributed OLTP database, and thus blessed to provide one-stop Hybrid Transactional/Analytical Processing (HTAP) solutions for online transactions and analyses.

It is an OLAP solution that runs Spark SQL directly on TiKV, the distributed storage engine.

The figure below show the architecture of TiSpark.

![image alt text](architecture.png)

TiSpark Architecture

+ TiSpark integrates well with the Spark Catalyst Engine. It provides precise control of computing, which allows Spark to read data from TiKV efficiently. It also supports index seek, which significantly improves the performance of the point query execution.
+ It utilizes several strategies to push down computing to reduce the size of dataset handling by Spark SQL, which accelerates query execution. It also uses the TiDB built-in statistical information for the query plan optimization.
+ From the perspective of data integration, TiSpark + TiDB provides a solution that performs both transaction and analysis directly on the same platform without building and maintaining any ETLs. It simplifies the system architecture and reduces the cost of maintenance.
+ In addition, you can deploy and utilize the tools from the Spark ecosystem for further data processing and manipulation on TiDB. For example, using TiSpark for data analysis and ETL, retrieving data from TiKV as a data source for machine learning, generating reports from the scheduling system and so on.

## Prerequisites for setting up TiSpark

+ The current version of TiSpark supports Spark 2.1. Spark 2.0 has not been fully tested on TiSpark yet. TiSpark does not support any versions earlier than 2.0, or later than 2.2.
+ TiSpark requires JDK 1.8+ and Scala 2.11 (Spark2.0 + default Scala version).
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

### For hybrid deployment of TiSpark and TiKV cluster

For the hybrid deployment of TiSpark and TiKV, add the resources required by TiSpark to the resources reserved in TiKV, and allocate 25% of the memory for the system.

## Deploy TiSpark

Download the TiSpark's jar package [here](https://github.com/pingcap/tispark/releases).

### Deploy TiSpark on the existing Spark cluster

You do not need to reboot the existing Spark cluster for TiSpark to run on it. Instead, use Spark's `--jars` parameter to introduce TiSpark as a dependency:

```
Spark-shell --jars $ PATH / tispark-${name_with_version}.jar
```

To deploy TiSpark as a default component, place the TiSpark jar package into each node's jar path on the Spark cluster and restart the Spark cluster:

```
$ {SPARK_INSTALL_PATH} / jars

```

In this way, you can use either `Spark-Submit` or `Spark-Shell` to use TiSpark directly.

### Deploy TiSpark without the Spark cluster

Without a Spark cluster, it is recommended that you use the Spark Standalone mode by placing a compiled version of Spark on each node on the cluster. For any problem, refer to the [official Spark website](https://spark.apache.org/docs/latest/spark-standalone.html). You are also welcome to [file an issue](https://github.com/pingcap/tispark/issues/new) on GitHub.

#### Step 1: Download and install

Download [Apache Spark](https://spark.apache.org/downloads.html).

+ For the Standalone mode without Hadoop support, use Spark **2.1.x** and any version of pre-build with Apache Hadoop 2.x with Hadoop dependencies.

+ If you need to use the Hadoop cluster, choose the corresponding Hadoop version. You can also build Spark from the [source code](https://spark.apache.org/docs/2.1.0/building-spark.html) to match the previous version of the official Hadoop 2.6.

> **Note:**
>
> TiSpark currently only supports Spark 2.1.x.

Suppose you already have a Spark binary, and the current PATH is `SPARKPATH`, copy the TiSpark jar package to the `$SPARKPATH/jars` directory.

#### Step 2: Start a Master node

Execute the following command on the selected Spark-Master node:

```
cd $ SPARKPATH

./sbin/start-master.sh
```

After the command is executed, a log file is printed on the screen. Check the log file to confirm whether the Spark-Master is started successfully.

Open the [http://spark-master-hostname:8080](http://spark-master-hostname:8080) to view the cluster information (if you do not change the default port number of Spark-Master).

When you start Spark-Slave, you can also use this panel to confirm whether the Slave is joined to the cluster.

#### Step 3: Start a Slave node

Similarly, start a Spark-Slave node by executing the following command:

```
./sbin/start-slave.sh spark: // spark-master-hostname: 7077
```

After the command returns, also check whether the Slave node is joined to the Spark cluster correctly from the panel.

Repeat the above command on all Slave nodes. After all the Slaves are connected to the Master, you have a Standalone mode Spark cluster.

#### Spark SQL shell and JDBC Server

To use JDBC server and interactive SQL shell, copy `start-tithriftserver.sh stop-tithriftserver.sh` to your Spark's `sbin` folder, and copy `tispark-sql` to the bin folder.

To start interactive shell:

```
./bin/tispark-sql
```

To use the Thrift Server, start the server in a similar way as starting the default Spark Thrift Server:

```
./sbin/start-tithriftserver.sh
```

To stop the server, run the following command:

```
./sbin/stop-tithriftserver.sh
```

## Unsupported MySQL Types

| Mysql Type |
| ----- |
| time |
| enum |
| set  |
| year |

## Demonstration

This section briefly introduces how to use Spark SQL for OLAP analysis (assuming that you have successfully started the TiSpark cluster as described above).

The following example uses a table named `lineitem` in the `tpch` database.

1. Add the entry below in your `./conf/spark-defaults.conf`, assuming that your PD node is located at `192.168.1.100`, port `2379`.

    ```
    spark.tispark.pd.addresses 192.168.1.100:2379
    ```

2. In the Spark-Shell, enter the following command:

    ```
    import org.apache.spark.sql.TiContext
    val ti = new TiContext (spark)
    ti.tidbMapDatabase ("tpch")
    ```

3. Call Spark SQL directly:

    ```
    spark.sql ("select count (*) from lineitem")
    ```

    The result:

    ```
    +-------------+
    | Count (1) |
    +-------------+
    | 600000000 |
    +-------------+
    ```

TiSpark's SQL Interactive shell is almost the same as spark-sql shell.

```
tispark-sql> use tpch;
Time taken: 0.015 seconds

tispark-sql> select count(*) from lineitem;
2000
Time taken: 0.673 seconds, Fetched 1 row(s)
```

For the JDBC connection with Thrift Server, try various JDBC-supported tools including SQuirreL SQL and hive-beeline.

For example, to use it with beeline:

```
./beeline
Beeline version 1.2.2 by Apache Hive
beeline> !connect jdbc:hive2://localhost:10000

1: jdbc:hive2://localhost:10000> use testdb;
+---------+--+
| Result  |
+---------+--+
+---------+--+
No rows selected (0.013 seconds)

select count(*) from account;
+-----------+--+
| count(1)  |
+-----------+--+
| 1000000   |
+-----------+--+
1 row selected (1.97 seconds)
```

## TiSparkR

TiSparkR is a thin layer built for supporting R language with TiSpark. Refer to [this document](../R/README.md) for TiSparkR guide.

## TiSpark on PySpark

TiSpark on PySpark is a Python package build to support Python language with TiSpark. Refer to [this document](../python/README.md) for TiSpark on PySpark guide .

## Use TiSpark with Hive

To use TiSpark with Hive:

1. Set the environment variable `HADOOP_CONF_DIR` to your Hadoop's configuration folder.

2. Copy `hive-site.xml` to the `spark/conf` folder before you start Spark.

```
val tisparkDF = spark.sql("select * from tispark_table").toDF
tisparkDF.write.saveAsTable("hive_table") // save table to hive
spark.sql("select * from hive_table a, tispark_table b where a.col1 = b.col1").show // join table across Hive and Tispark
```

## Load Spark DataFrame into TiDB using JDBC

TiSpark does not provide a direct way to load data into your TiDB cluster, but you can still do it by using JDBC.

For example:

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
.option("isolationLevel", "None") // set isolationLevel to NONE
.option("user", "root") // TiDB user here
.save()
```

Please set `isolationLevel` to `NONE` to avoid large single transactions which might lead to TiDB OOM.

## Statistics information

TiSpark uses the statistic information for:

+ Determining which index to use in your query plan with the lowest estimated cost.
+ Small table broadcasting, which enables efficient broadcast join.

For TiSpark to use the statistic information, first make sure that relevant tables have been analyzed.

See [here](https://github.com/pingcap/docs/blob/master/sql/statistics.md) for more details about how to analyze tables.

Load the statistics information from your storage.

```scala
val ti = new TiContext(spark)

// Map the databases to be used.
// You can specify whether to load statistics information automatically during the database mapping in your configuration file described as below.
// The statistics information is loaded automatically by default, which is recommended in most cases.
ti.tidbMapDatabase("db_name")

// Another option is to manually load it by yourself, but this is not the recommended way.

// First, get the table from which you want to load statistics information.
val table = ti.meta.getTable("db_name", "tb_name").get

// If you want to load statistics information of all columns, use:
ti.statisticsManager.loadStatisticsInfo(table)

// To use the statistics information of some columns, use:
ti.statisticsManager.loadStatisticsInfo(table, "col1", "col2", "col3")

// You can specify required columns by `vararg`

// Collect the statistic information of other tables.

// Then, you can query as usual. TiSpark uses the statistic information collected to optimize index selection and broadcast join.
```

> **Note:**
>
> Table statistics is cached in your Spark driver node's memory, so you need to make sure that the memory is large enough for the statistics information.

Currently, you can adjust these configurations in your `spark.conf` file.

| Property Name | Default | Description
| --------   | -----:   | :----: |
| `spark.tispark.statistics.auto_load` | `true` | Whether to load the statistics information automatically during database mapping |

## Common port numbers used by Spark cluster

|Port Name| Default Port Number   | Configuration Property   | Notes|
|---------------| ------------- |-----|-----|
|Master web UI  | `8080`  | spark.master.ui.port  or SPARK_MASTER_WEBUI_PORT| The value set by `spark.master.ui.port` takes precedence.  |
|Worker web UI  |  `8081`  | spark.worker.ui.port or SPARK_WORKER_WEBUI_PORT  | The value set by `spark.worker.ui.port` takes precedence.|
|History server web UI   |  `18080`  | spark.history.ui.port  |Optional; it is only applied if you use the history server.   |
|Master port   |  `7077`  |   SPARK_MASTER_PORT  |   |
|Master REST port   |  `6066`  | spark.master.rest.port  | Not needed if you disable the `REST` service.   |
|Worker port |  (random)   |  SPARK_WORKER_PORT |   |
|Block manager port  |(random)   | spark.blockManager.port  |   |
|Shuffle server  |  `7337`   | spark.shuffle.service.port  |  Optional; it is only applied if you use the external shuffle service.  |
|  Application web UI  |  `4040`  |  spark.ui.port | If `4040` has been occupied, then `4041` is used. |

## FAQ

Q: What are the pros and cons of independent deployment as opposed to a shared resource with an existing Spark / Hadoop cluster?

A: You can use the existing Spark cluster without a separate deployment, but if the existing cluster is busy, TiSpark will not be able to achieve the desired speed.

Q: Can I mix Spark with TiKV?

A: If TiDB and TiKV are overloaded and run critical online tasks, consider deploying TiSpark separately.

You also need to consider using different NICs to ensure that OLTP's network resources are not compromised so that online business is not affected.

If the online business requirements are not high or the loading is not large enough, you can mix TiSpark with TiKV deployment.

Q: How to use PySpark with TiSpark?

A: Follow [TiSpark on PySpark](../python/README_spark2.1.md).

Q: How to use SparkR with TiSpark?

A: Follow [TiSpark on SparkR](../R/README.md).
