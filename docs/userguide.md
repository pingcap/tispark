# TiSpark (Beta) User Guide

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

+ The current version of TiSpark supports Spark 2.1. For Spark 2.0 and Spark 2.2, it has not been fully tested yet . It does not support any versions earlier than 2.0.
+ TiSpark requires JDK 1.8+ and Scala 2.11 (Spark2.0 + default Scala version).
+ TiSpark runs in any Spark mode such as YARN, Mesos, and Standalone.


## Recommended configuration

### Deployment of TiKV and TiSpark clusters

#### Configuration of the TiKV cluster

For independent deployment of TiKV and TiSpark, it is recommended to refer to the following recommendations
 
+ Hardware configuration
 - For general purposes, please refer to the TiDB and TiKV hardware configuration [recommendations](https://github.com/pingcap/docs/blob/master/op-guide/recommendation.md#deployment-recommendations).
 - If the usage is more focused on the analysis scenarios, you can increase the memory of the TiKV nodes to at least 64G. If using  Hard Disk Drive (HDD), it is recommended to use at least 8 disks.

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

Generally, it is  recommended to allocat 32G memory for Spark. Please reserve at least 25% of the memory for the operating system and buffer cache.

It is recommended to provision at least 8 to 16 cores on per machine for Spark. Initially, you can assign all the CPU cores to Spark.

Please refer to the Spark official configuration website at (https://spark.apache.org/docs/latest/spark-standalone.html). The following is an example based on the spark-env.sh configuration:

```
SPARK_EXECUTOR_MEMORY = 32g
SPARK_WORKER_MEMORY = 32g
SPARK_WORKER_CORES = 8
```

#### Hybrid deployment configuration for the TiSpark and TiKV cluster

For the  hybrid deployment of TiSpark and TiKV, add the TiSpark required resources to the TiKV reserved resources, and allocate 25% of the memory for the system.
 

## Deploy TiSpark

Download the TiSpark's jar package [here] (https://download.pingcap.org/tispark-0.1.0-beta-SNAPSHOT-jar-with-dependencies.jar).

### Deploy TiSpark on the existing Spark cluster

Running TiSpark on an existing Spark cluster does not require a reboot of the cluster. You can use Spark's `--jars` parameter to introduce TiSpark as a dependency:

```
Spark-shell --jars $ PATH / tispark-0.1.0.jar
```

If you want to deploy TiSpark as a default component, simply place the TiSpark jar package into the jars path for each node of the Spark cluster and restart the Spark cluster:

```
$ {SPARK_INSTALL_PATH} / jars

```

In this way,  you can use either `Spark-Submit` or `Spark-Shell` to use TiSpark directly.


### Deploy TiSpark without the Spark cluster


If you do not have a Spark cluster, we recommend using the standalone mode. To use the Spark Standalone model, you can simply place a compiled version of Spark on each node of the cluster. If you encounter problems, please to refer to its official website* (https://spark.apache.org/docs/latest/spark-standalone.html)*. And you are welcome to [file an issue](https://github.com/pingcap/tispark/issues/new) on our GitHub.


#### Download and install

You can download [Apache Spark](https://spark.apache.org/downloads.html)

For the Standalone mode without Hadoop support, use Spark 2.1.x and any version of Pre-build with Apache Hadoop 2.x with Hadoop dependencies. If you need to use the Hadoop cluster, please choose the corresponding Hadoop version. You can also choose to build from the [source code](https://spark.apache.org/docs/2.1.0/building-spark.html) to match the previous version of the official Hadoop 2.6. Please note that TiSpark currently only supports Spark 2.1.x version.
 
Suppose you already have a Spark binaries, and the current PATH is `SPARKPATH`, please copy the TiSpark jar package to the `$ {SPARKPATH} / jars` directory.

#### Starting a Master node

Execute the following command on the selected Spark Master node:
 
```
cd $ SPARKPATH

./sbin/start-master.sh  
```

After the above step is completed, a log file will be printed on the screen. Check the log file to confirm whether the Spark-Master is started successfully. You can open the [http://spark-master-hostname:8080](http://spark-master-hostname:8080) to view the cluster information (if you does not change the Spark-Master default port number). When you start Spark-Slave, you can also use this panel to confirm whether the Slave is  joined to the cluster.

#### Starting a Slave node


Similarly, you can start a Spark-Slave node with the following command:

```
./sbin/start-slave.sh spark: // spark-master-hostname: 7077
```

After the command returns, you can see if the Slave node is joined to the Spark cluster correctly from the panel as well. Repeat the above command at all Slave nodes. After all Slaves are connected to the master, you have a Standalone mode Spark cluster.

#### Spark SQL shell and JDBC Server

If you want to use JDBC server and interactive SQL shell, please copy `start-tithriftserver.sh stop-tithriftserver.sh` to your Spark's sbin folder and `tispark-sql` to bin folder. 

To start interactive shell:
```
./bin/tispark-sql
```

To use Thrift Server, you can start it similar way as default Spark Thrift Server:
```
./sbin/start-tithriftserver.sh
```

And stop it like below:
```
./sbin/stop-tithriftserver.sh
```


## Demo

Assuming you have successfully started the TiSpark cluster as described above, here's a quick introduction to how to use Spark SQL for OLAP analysis. Here we use a table named `lineitem` in the `tpch` database as an example.

Add 
```
spark.tispark.pd.addresses 192.168.1.100:2379
```
entry in your ./conf/spark-defaults.conf, assuming that your PD node is located at `192.168.1.100`, port `2379`:

In the Spark-Shell, enter the following command:

```
import org.apache.spark.sql.TiContext
val ti = new TiContext (spark)
ti.tidbMapDatabase ("tpch")

```
After that you can call Spark SQL directly:

```
spark.sql ("select count (*) from lineitem")
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
tispark-sql> use tpch;
Time taken: 0.015 seconds

tispark-sql> select count(*) from lineitem;
2000
Time taken: 0.673 seconds, Fetched 1 row(s)
```

For JDBC connection with Thrift Server, you can try it with various JDBC supported tools including SQuirreLSQL and hive-beeline.
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
A thin layer build for supporting R language with TiSpark
Please refer to [this document](../TiSparkR/README.md) for usage.

## FAQ

Q: What are the pros/cons of independent deployment as opposed to a shared resource with an existing Spark / Hadoop cluster?

A: You can use the existing Spark cluster without a separate deployment, but if the existing cluster is busy, TiSpark will not be able to achieve the desired speed. 

Q: Can I mix Spark with TiKV?

A: If TiDB and TiKV are overloaded and run critical online tasks, consider deploying TiSpark separately. You also need to consider using different NICs to ensure that OLTP's network resources are not compromised and affect online business. If the online business requirements are not high or the loading is not large enough, you can consider mixing TiSpark with TiKV deployment.
