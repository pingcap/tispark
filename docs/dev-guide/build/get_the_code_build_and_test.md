# Get the code, build, and test
Prerequisites
1. Please make sure you have installed `Scala`, `Java`, `Maven` and deploy a TiDB cluster.
2. Please make sure you have installed `Git`.
3. If you want to run the code, please make sure you have installed spark.

## Clone
```
git clone https://github.com/pingcap/tispark.git
```
## Build
```
cd tispark
mvn clean install -Dmaven.test.skip=true
```
If you see the following message, it means the building succeeds.
```
[INFO] Reactor Summary for TiSpark Project Parent POM 3.1.0-SNAPSHOT:
[INFO]
[INFO] TiSpark Project Parent POM ......................... SUCCESS [  0.782 s]
[INFO] TiSpark Project TiKV Java Client ................... SUCCESS [ 38.538 s]
[INFO] Database Random Test Framework ..................... SUCCESS [ 16.137 s]
[INFO] TiSpark Project Core Internal ...................... SUCCESS [ 37.893 s]
[INFO] TiSpark Project Spark Wrapper Spark-3.0 ............ SUCCESS [ 10.942 s]
[INFO] TiSpark Project Spark Wrapper Spark-3.1 ............ SUCCESS [  9.735 s]
[INFO] TiSpark Project Spark Wrapper Spark-3.2 ............ SUCCESS [ 10.988 s]
[INFO] TiSpark Project Assembly ........................... SUCCESS [  5.175 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```
Then you will find a jar under `assembly/target/` which is named 
`tispark-assembly-${spark_version}_${scala_version}-${tispark_version}-SNAPSHOT.jar`

## Run Test

> Make sure you have deployed the TiDB cluster. see [here](./start_tidb_cluster.md) on how to start a TiDB cluster.

The tests are under `test` directories in each module.

Please make sure you have added the tests for your bug fix or feature.

About how to test, see [here](../../../core/src/test/Readme.md).