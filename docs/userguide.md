# TiSpark (version >= 2.0) User Guide

**Note: This is a user guide for TiSpark version >= 2.0. If you are using version before 2.0, please refer to [Document for Spark 2.1](./userguide_spark2.1.md)**

TiSpark is a thin layer built for running Apache Spark on top of TiDB/TiKV to answer the complex OLAP queries. It takes advantages of both the Spark platform and the distributed TiKV cluster, at the same time, seamlessly glues to TiDB, the distributed OLTP database, to provide a Hybrid Transactional/Analytical Processing (HTAP) solution to serve as a one-stop solution for online transactions and analysis.


TiSpark depends on the TiKV cluster and the PD cluster. It also needs to set up a Spark cluster. This document provides a brief introduction to how to setup and use TiSpark. It requires some basic knowledge of Apache Spark. For more information, please refer to [Spark website](https://spark.apache.org/docs/latest/index.html).


## Overview

TiSpark is an OLAP solution that runs Spark SQL directly on TiKV, the distributed storage engine.

![image alt text](architecture.png)


TiSpark Architecture

+ TiSpark integrates with Spark Catalyst Engine deeply. It provides precise control of the computing, which allows Spark read data from TiKV efficiently. It also supports index seek, which improves the performance of the point query execution significantly.
+ It utilizes several strategies to push down the computing to reduce the size of dataset handling by Spark SQL, which accelerates the query execution. It also uses the TiDB built-in statistical information for the query plan optimization.
+ From the data integration point of view, TiSpark + TiDB provides a solution runs both transaction and analysis directly on the same platform without building and maintaining any ETLs. It simplifies the system architecture and reduces the cost of maintenance.
+ In addition, you can deploy and utilize tools from the Spark ecosystem for further data processing and manipulation on TiDB. For example, using TiSpark for data analysis and ETL; retrieving data from TiKV as a machine learning data source; generating reports from the scheduling system and so on.

## Environment Setup

+ The current version of TiSpark supports Spark 2.3+/2.4+. It does not support any versions earlier than Spark 2.3.
+ TiSpark requires JDK 1.8+ and Scala 2.11 (Spark2.0 + default Scala version).
+ TiSpark runs in any Spark mode such as YARN, Mesos, and Standalone.


## Recommended configuration

### Deployment of TiKV and TiSpark clusters

#### Configuration of the TiKV cluster

For independent deployment of TiKV and TiSpark, it is recommended to refer to the following recommendations
 
+ Hardware configuration
 - For general purposes, please refer to the TiDB and TiKV hardware configuration [recommendations](https://github.com/pingcap/docs/blob/master/op-guide/recommendation.md#deployment-recommendations).
 - If the usage is more focused on the analysis scenarios, you can increase the memory of the TiKV nodes to at least 64G. If using Hard Disk Drive (HDD), it is recommended to use at least 8 disks.

+ TiKV parameters (default)

```
[Server]
End-point-concurrency = 8 # For OLAP scenarios, consider increasing this parameter
[Raftstore]
Sync-log = false


[Rocksdb]
Max-background-compactions = 6
Max-background-flushes = 2

[Rocksdb.defaultcf]
Block-cache-size = "10GB"

[Rocksdb.writecf]
Block-cache-size = "4GB"

[Rocksdb.raftcf]
Block-cache-size = "1GB"

[Rocksdb.lockcf]
Block-cache-size = "1GB"

[Storage]
Scheduler-worker-pool-size = 4
```

#### Configuration of the independent deployment of the Spark cluster and the TiSpark cluster 

 
Please refer to the [Spark official website](https://spark.apache.org/docs/latest/hardware-provisioning.html) for the detail hardware recommendations.

The following is a short overview of the TiSpark configuration.

Generally, it is recommended to allocate 32G memory for Spark. Please reserve at least 25% of the memory for the operating system and buffer cache.

It is recommended to provision at least 8 to 16 cores on per machine for Spark. Initially, you can assign all the CPU cores to Spark.

Please refer to the Spark official configuration website at (https://spark.apache.org/docs/latest/spark-standalone.html). The following is an example based on the spark-env.sh configuration:

```
SPARK_EXECUTOR_MEMORY = 32g
SPARK_WORKER_MEMORY = 32g
SPARK_WORKER_CORES = 8
```

In the spark-defaults.conf, you should add following lines:
```
spark.tispark.pd.addresses $your_pd_servers
spark.sql.extensions org.apache.spark.sql.TiExtensions
```
`your_pd_servers` should be comma-separated pd addresses, each in the format of `$your_pd_address:$port`

For example, `10.16.20.1:2379,10.16.20.2:2379,10.16.20.3:2379` when you have multiple PD servers on 10.16.20.1,10.16.20.2,10.16.20.3 with port 2379.

#### Hybrid deployment configuration for the TiSpark and TiKV cluster

For the hybrid deployment of TiSpark and TiKV, add the TiSpark required resources to the TiKV reserved resources, and allocate 25% of the memory for the system.
 

## Deploy TiSpark

Download the TiSpark's jar package [here](http://download.pingcap.org/tispark-latest-linux-amd64.tar.gz).

### Deploy TiSpark on the existing Spark cluster

Running TiSpark on an existing Spark cluster does not require a reboot of the cluster. You can use Spark's `--jars` parameter to introduce TiSpark as a dependency:

```
spark-shell --jars $your_path_to/tispark-${name_with_version}.jar
```

If you want to deploy TiSpark as a default component, simply place the TiSpark jar package into the jars path for each node of the Spark cluster and restart the Spark cluster:

```
cp $your_path_to/tispark-${name_with_version}.jar $SPARK_HOME/jars
```

In this way, you can use either `Spark-Submit` or `Spark-Shell` to use TiSpark directly.


### Deploy TiSpark without the Spark cluster


If you do not have a Spark cluster, we recommend using the Spark Standalone mode by placing a compiled version of Spark on each node on the cluster. If you encounter problems, please to refer to the [official Spark website](https://spark.apache.org/docs/latest/spark-standalone.html). You are also welcome to [file an issue](https://github.com/pingcap/tispark/issues/new) on our GitHub.


#### Download and install

You can download [Apache Spark](https://spark.apache.org/downloads.html)

For the Standalone mode without Hadoop support, use Spark **2.3.x,2.4.x** and any version of Pre-build with Apache Hadoop 2.x with Hadoop dependencies.

If you need to use the Hadoop cluster, please choose the corresponding Hadoop version. You can also choose to build Spark from the [Spark 2.3 source code](https://spark.apache.org/docs/2.3.3/building-spark.html) or [Spark 2.4 source code](https://spark.apache.org/docs/2.4.1/building-spark.html) to match the previous version of the official Hadoop 2.6.

**Please confirm the Spark version your TiSpark version supports.**

Suppose you already have a Spark binaries, and the current PATH is `SPARKPATH`, please copy the TiSpark jar package to the `$SPARKPATH/jars` directory.

#### Starting a Master node

Execute the following command on the selected Spark Master node:
 
```
cd $SPARKPATH

./sbin/start-master.sh  
```

After the above step is completed, a log file will be printed on the screen. Check the log file to confirm whether the Spark-Master is started successfully. You can open the [http://spark-master-hostname:8080](http://spark-master-hostname:8080) to view the cluster information (if you does not change the Spark-Master default port number). When you start Spark-Slave, you can also use this panel to confirm whether the Slave is joined to the cluster.

#### Starting a Slave node


Similarly, you can start a Spark-Slave node with the following command:

```
./sbin/start-slave.sh spark://spark-master-hostname:7077
```

After the command returns, you can see if the Slave node is joined to the Spark cluster correctly from the panel as well. Repeat the above command at all Slave nodes. After all Slaves are connected to the master, you have a Standalone mode Spark cluster.

#### Spark SQL shell and JDBC Server

Now that TiSpark supports Spark 2.3/2.4, you can use Spark's ThriftServer and SparkSQL directly.


## Demo

Assuming you have successfully started the TiSpark cluster as described above, here's a quick introduction to how to use Spark SQL for OLAP analysis. Here we use a table named `lineitem` in the `tpch` database as an example.

Add 
```
spark.tispark.pd.addresses 192.168.1.100:2379
spark.sql.extensions org.apache.spark.sql.TiExtensions
```
entry in your ./conf/spark-defaults.conf, assuming that your PD node is located at `192.168.1.100`, port `2379`:

In the Spark-Shell, enter the following command:
```
spark.sql("use tpch")
```
After that you can call Spark SQL directly:

```
spark.sql("select count (*) from lineitem").show
```

The result is:

```
+-------------+
| Count (1) |
+-------------+
| 600000000 |
+-------------+
```
 
TiSpark's SQL Interactive shell is almost the same as spark-sql shell.

```
spark-sql> use tpch;
Time taken: 0.015 seconds

spark-sql> select count(*) from lineitem;
2000
Time taken: 0.673 seconds, Fetched 1 row(s)
```

For JDBC connection with Thrift Server, you can try it with various JDBC supported tools including SQuirreL SQL and hive-beeline.
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
TiSparkR is a thin layer built for supporting R language with TiSpark
Refer to [this document](../R/README.md) for usage.

## TiSpark on PySpark
TiSpark on PySpark is a Python package build to support the Python language with TiSpark. 
Refer to [this document](../python/README.md) for usage.

## Use TiSpark together with Hive
TiSpark should be ok to use together with Hive. 
You need to set environment variable HADOOP_CONF_DIR to your Hadoop's configuration folder and copy hive-site.xml to spark/conf folder before starting Spark.
```
val tisparkDF = spark.sql("select * from tispark_table").toDF
tisparkDF.write.saveAsTable("hive_table") // save table to hive
spark.sql("select * from hive_table a, tispark_table b where a.col1 = b.col1").show // join table across Hive and Tispark
```

## Load Spark DataFrame into TiDB using JDBC
TiSpark does not provide a direct way to load data into your TiDB cluster, but you can still load using jdbc like this:
```scala
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

val customer = spark.sql("select * from customer limit 100000")
// you might repartition source to make it balance across nodes
// and increase concurrency
val df = customer.repartition(32)
df.write
.mode(saveMode = "append")
.format("jdbc")
.option("driver", "com.mysql.jdbc.Driver")
 // replace host and port as your and be sure to use rewrite batch
.option("url", "jdbc:mysql://127.0.0.1:4000/test?rewriteBatchedStatements=true")
.option("useSSL", "false")
// As tested, 150 is good practice
.option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 150)
.option("dbtable", s"cust_test_select") // database name and table name here
.option("isolationLevel", "NONE") // recommended to set isolationLevel to NONE if you have a large DF to load.
.option("user", "root") // TiDB user here
.save()
``` 
It is recommended to set `isolationLevel` to `NONE` to avoid large single transactions which may potentially lead to TiDB OOM.

## Statistics information
TiSpark could use TiDB's statistic information for 

1. Determining which index to ues in your query plan with the estimated lowest cost.
2. Small table broadcasting, which enables efficient broadcast join.

If you would like TiSpark to use statistic information, first you need to make sure that concerning tables have already been analyzed. Read more about how to analyze tables [here](https://github.com/pingcap/docs/blob/master/sql/statistics.md).

Since TiSpark 2.0, statistics information will be default to auto load.

Note that table statistics will be cached in your spark driver node's memory, so you need to make sure that your memory should be enough for your statistics information.
Currently you could adjust these configs in your spark.conf file.
  
| Property Name | Default | Description
| --------   | -----:   | :----: |
| spark.tispark.statistics.auto_load | true | Whether to load statistics info automatically during database mapping. |

## Reading partition table from TiDB
Currently, only range partition table is limited supported. If partition expression having function expression 
rather than `year` then partition pruning will not be applied. Such scan can be considered full table scan if there is no index in the schema. 

## Common Port numbers used by Spark Cluster
|Port Name| Default Value Port Number   | Configuration Property   | Notes|
|---------------| ------------- |-----|-----|
|Master web UI		  | 8080  | spark.master.ui.port  or SPARK_MASTER_WEBUI_PORT| The value set by the spark.master.ui.port property takes precedence.  |
|Worker web UI  |   8081| spark.worker.ui.port or SPARK_WORKER_WEBUI_PORT  | The value set by the spark.worker.ui.port takes precedence.|
|History server web UI   |18080   | spark.history.ui.port	  |Optional; only applies if you use the history server.   |
|Master port   | 7077  | 		SPARK_MASTER_PORT	  |   |
|Master REST port   | 6066  | spark.master.rest.port  | Not needed if you disable the REST service.   |
|Worker port | 	(random)   |  SPARK_WORKER_PORT |   |
|Block manager port  |(random)   | spark.blockManager.port  |   |
|Shuffle server	  |7337   | spark.shuffle.service.port  | 		Optional; only applies if you use the external shuffle service.  |
|Application web UI	|4040|	spark.ui.port | if 4040 is used, then 4041 will be used

## FAQ

Q: What are the pros/cons of independent deployment as opposed to a shared resource with an existing Spark / Hadoop cluster?

A: You can use the existing Spark cluster without a separate deployment, but if the existing cluster is busy, TiSpark will not be able to achieve the desired speed. 

Q: Can I mix Spark with TiKV?

A: If TiDB and TiKV are overloaded and run critical online tasks, consider deploying TiSpark separately. You also need to consider using different NICs to ensure that OLTP's network resources are not compromised and affect online business. If the online business requirements are not high or the loading is not large enough, you can consider mixing TiSpark with TiKV deployment.

Q: How to use PySpark with TiSpark?

A: Please follow [TiSpark on PySpark](../python/README.md).

Q: How to use SparkR with TiSpark?

A: Please follow [TiSpark on SparkR](../R/README.md).
