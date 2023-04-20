# TiSpark

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.pingcap.tispark/tispark-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.pingcap.tispark/tispark-parent)
[![License](https://img.shields.io/github/license/pingcap/tispark.svg)](https://github.com/pingcap/tispark/blob/master/LICENSE)

TiSpark is a thin layer built for running Apache Spark on top of TiDB/TiKV/TiFlash to answer complex OLAP queries. It enjoys the merits of both the Spark platform and the distributed clusters of TiKV/TiFlash while seamlessly integrated to TiDB.

The figure below show the architecture of TiSpark.

![architecture](./docs/architecture.png)

+ TiSpark integrates well with the Spark Catalyst Engine. It provides precise control of computing, which allows Spark to read data from TiKV efficiently. It also supports index seek, which significantly improves the performance of the point query execution.
+ It utilizes several strategies to push down computing to reduce the size of dataset handling by Spark SQL, which accelerates query execution. It also uses the TiDB built-in statistical information for the query plan optimization.
+ From the perspective of data integration, TiSpark + TiDB provides a solution that performs both transaction and analysis directly on the same platform without building and maintaining any ETLs. It simplifies the system architecture and reduces the cost of maintenance.
+ In addition, you can deploy and utilize the tools from the Spark ecosystem for further data processing and manipulation on TiDB. For example, using TiSpark for data analysis and ETL, retrieving data from TiKV as a data source for machine learning, generating reports from the scheduling system and so on.

TiSpark relies on the availability of TiKV clusters and PDs. You also need to set up and use the Spark clustering platform.

Most of the TiSpark logic is inside a thin layer, namely, the [tikv-client](https://github.com/pingcap/tispark/tree/master/tikv-client) library.

## Doc TOC

- [User Guide](docs/userguide_3.0.md)
- [Dev Guide](https://github.com/pingcap/tispark/wiki/Dev-Guide)
- [Benchmark](https://github.com/pingcap/tispark/wiki/TiSpark-Benchmark)

## About mysql-connector-java

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

## Feature Support

| Feature Support                   | TiSpark 2.4.x | TiSpark 2.5.x | TiSpark 3.0.x  | TiSpark master |
| --------------------------------- |---------------|---------------|----------------|-----------------
| SQL select without tidb_catalog   | &#10004;      | &#10004;      |                |                |
| SQL select with tidb_catalog      |               | &#10004;      | &#10004;       | &#10004;       |
| SQL delete from with tidb_catalog |               |               | &#10004;       | &#10004;       |
| DataFrame append                  | &#10004;      | &#10004;      | &#10004;       | &#10004;       |
| DataFrame reads                   | &#10004;      | &#10004;      | &#10004;       | &#10004;       |

see [here](https://github.com/pingcap/tispark/wiki/Feature-Support-Detail) for more detail.

## Limitations

- TiDB starts to support `view` since `tidb-3.0`. TiSpark currently **does not support** `view`. Users are not be able to observe or access data through `view` with TiSpark.

- Spark config `spark.sql.runSQLOnFiles` should not be set to `false`, or you may got `Error in query: Table or view not found` error.

- Using the style of "{db}.{table}.{colname}" in the condition is not supported, e.g. `select * from t where db.t.col1 = 1`.

- `Null in aggregration` is not supported, e.g. `select sum(null) from t group by col1`.

- The dependency `tispark-assembly` should not be packaged into `JAR of JARS` file (for example, build with spring-boot-maven-plugin), or you will get `ClassNotFoundException`. You can solve it by adding `spark-wrapper-spark-version` in your dependency or constructing another forms of jar file.

- TiSpark doesn't support GBK character set.

- TiSpark doesn't support the whole collations rule. Currently, TiSpark only supports the following collations: utf8_bin, utf8_general_ci, utf8_unicode_ci, utf8mb4_bin, utf8mb4_general_ci and utf8mb4_unicode_ci.

- If `spark.sql.ansi.enabled` is false an overflow of sum(bigint) will not cause an error but “wrap” the result, or you can cast bigint to decimal to avoid the overflow.

- TiSpark supports retrieving data from table with `Expression Index`, but the `Expression Index` will not be used by the planner of TiSpark.

## Follow us

### Twitter

[@PingCAP](https://twitter.com/PingCAP)

### Forums

For English users, go to [TiDB internals](https://internals.tidb.io).

For Chinese users, go to [AskTUG](https://asktug.com).

## License

TiSpark is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
