# Get the code, build, and run
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
If you build it success, you also have downloaded needed files.
Tests are required when add new features or fix bugs.
You can add tests or modify tests according your codes.
These tests are under `test` directories in each module.

Before running test, you need to make a copy of `tidb_config.properties.template` and rename it to
`tidb_config.properties` and update to configuration as yours and make sure you have deployed a TiDB cluster.


## Run Jar
Please make sure you have installed spark before running TiSpark and add the following configuration in `spark-defaults.conf`.
```
spark.sql.extensions  org.apache.spark.sql.TiExtensions
spark.tispark.pd.addresses  ${your_pd_adress}
spark.sql.catalog.tidb_catalog  org.apache.spark.sql.catalyst.catalog.TiCatalog
spark.sql.catalog.tidb_catalog.pd.addresses  ${your_pd_adress}
```

And then you can use TiSpark in `spark-shell` by run following command.

```
spark-shell --jars tispark-assembly-${spark_version}_${scala_version}-${tispark_version}-SNAPSHOT.jar
```

Spark will load the jar when running. But we can't use the feature TiSpark provides now. We implement it using catalog . Please run the following command to use TiSpark.

```
spark.sql("use tidb_catalog")
```