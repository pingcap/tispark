## Summary

TiSpark is an OLAP solution that runs Spark SQL directly on the distributed storage engine TiKV. TiSpark depends on the existence of TiKV clusters and PDs. It also needs to setup a Spark clustering platform. This document provides a quick introduction of how to setup and use TiSpark. It requires some basic knowledge of Apache Spark. For more information, please refer to Spark website (https://spark.apache.org/docs/latest/index.html).

##Environment Setup

The current version of TiSpark supports Spark 2.1, and has not been fully tested for Spark 2.0 and Spark 2.2. It does not support any versions below 2.0 

TiSpark requires JDK 1.8+ and Scala 2.11 (Spark2.0 + default Scala version).

TiSpark runs in any Spark mode such as YARN, Mesos, Standalone

Recommended configuration

Deployment of TiKV and TiSpark clusters

Configuration of TiKV cluster

For independent set of TiKV and TiSpark deployment, it is recommended to refer to the following recommendations

•	Hardware configuration
For general purpose, please refer to the TiDB and TiKV hardware configuration recommendations (http:t.cn/R9wzcdM ) .

If the usage is more focus on the analysis propose, you can add TiKV nodes to at least 64G memory, if using mechanical hard disk, it is recommended 8 disks minimum.

•	TiKV parameters (default)
[Server]
End-point-concurrency = 8 # If analysis focus, consider expanding this parameter
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

 Configuration of Spark / TiSpark cluster running on independent set

Please refer to the Spark official website (https://spark.apache.org/docs/latest/hardware-provisioning.html) for the detail hardware recommendations. 

The following is a short overview of the TiSpark configuration.

General, Spark recommended allocating 32G memory. Please reserve at least 25% of the memory for the operating system and buffer cache.

Spark recommends provision at least 8 to 16 cores on per machine. Initially, you can assign all CPU cores to Spark.
Please refer to the Spark official configuration website at (https://spark.apache.org/docs/latest/spark-standalone.html) . The following is an example based on the spark-env.sh configuration:
SPARK_EXECUTOR_MEMORY = 32g
SPARK_WORKER_MEMORY = 32g
SPARK_WORKER_CORES = 8

Hybrid deployment configuration for TiSpark and TiKV cluster

For TiKV, TiSpark hybrid deployment, add the resources of Spark required and allocate 25% of the memory apart from the original TiKV reserved for the system itself.
  
 Deploy TiSpark
TiSpark's jar package can be downloaded here (https://download.pingcap.org/tispark-0.1.0-beta-SNAPSHOT-jar-with-dependencies.jar) .

Deploying TiSpark on existing Spark cluster

Running TiSpark on an existing Spark cluster does not require a reboot of the cluster. You can use Spark's --jars parameter to introduce TiSpark as a dependency:

Spark-shell --jars $ PATH / tispark-0.1.0.jar

If you want to deploy TiSpark as a default component, simply place the TiSpark jar package into the jars path for each node of the Spark clusters and restart the Spark clusters:

$ {SPARK_INSTALL_PATH} / jars

Whether using Spark-Submit or Spark-Shell, TiSpark can be used directly.

Deploying TiSpark without Spark cluster

If you do not have the Spark cluster, we recommend installing Spark standalone model to a cluster. To install Spark Standalone model, you simply place a compiled version of Spark on each node on the cluster. If you encounter problems, please to refer to its official website (https://spark.apache.org/docs/latest/spark-standalone.html). And you are more than welcome to register an issue on our GitHub.

Download the installation package and install it

You can download Apache Spark here.

For Standalone mode without supporting from Hadoop, use Spark 2.1.x and any version of Pre-build with Apache Hadoop 2.x with Hadoop dependencies. If you need to use the Hadoop cluster, please choose the corresponding Hadoop version. You can also choose to build from the source code (https://spark.apache.org/docs/2.1.0/building-spark.html) to match the previous version of the official Hadoop 2.6. Please note that TiSpark currently only supports Spark 2.1.x version.

Suppose you already have a Spark binaries, and the current PATH is SPARKPATH, please copy the TiSpark jar package to the $ {SPARKPATH} / jars directory.

Starting Master
Execute the following command on the selected Spark Master node:

Cd $ SPARKPATH
./sbin/start-master.sh  

After the above step is completed, a log file will be printed on the screen. Check the log file to confirm whether the Spark-Master started successfully. You can open the http: // spark-master-hostname: 8080 to view the cluster information (if you did not change the Spark-Master default port numebr). When you start Spark-Slave, you can also use this panel to confirm whether the Slave has joined the cluster.

Starting Slave

Similarly, you can start the Spark-Slave node with the following command:

./sbin/start-slave.sh spark: // spark-master-hostname: 7077

After the command returns, you can see if the Slave has joined the Spark cluster correctly from the panel as well. Repeat the above command at all Slave nodes. After confirming that all Slaves are properly connected to the master, then you have a Standalone mode Spark cluster.
  
 Demo
Assuming you have successfully started the TiSpark cluster as described above, here's a quick introduction to how to use Spark SQL for OLAP analysis. Here we use a table named as lineitem on database tpch as an example.

In the Spark-Shell, enter the following command, assuming that your PD node is located at 192.168.1.100, port 2379:
Import org.apache.spark.sql.TiContext
Val ti = new TiContext (spark, List ("192.168.1.100:2379")
Ti.tidbMapDatabase ("tpch")
After that you can call Spark SQL directly
Spark.sql ("select count (*) from lineitem")

The result is:
+ ------------- +
Count (1) |
+ ------------- +
| 600000000 |
+ ------------- +
  
FAQ
Q. What is the pro/con of independent deployment or a shared resource with an existing Spark / Hadoop cluster?
A. You can use the existing Spark cluster without a separate deployment, but if the existing cluster is busy, TiSpark will not be able to achieve the desired speed.

Q. Can I mix Spark with TiKV?
A. If TiDB and TiKV are overloaded and run critical online tasks, consider deploying TiSpark separately; and consider using different NICs to ensure that OLTP's network resources are not compromised and affect online business. If the online business requirements are not high or the loading is not large enough, you can consider mixing TiSpark with TiKV deployment.


