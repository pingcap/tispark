# TiDB Data Source API User Guide
The TiDB Connector for Spark enables using TiDB as an Apache Spark data source, similar to other data sources (PostgreSQL, HDFS, S3, etc.).

The TiDB connector support spark-2.3.0+.

## Interaction Between TiDB and Spark
The connector supports bi-directional data movement between TiDB and Spark cluster.

Using the connector, you can perform the following operations:
  - Populate a Spark DataFrame from a table in TiDB.
  - Write the contents of a Spark DataFrame to a table in TiDB.

## Query Pushdown
For optimal performance, you typically want to avoid reading lots of data or transferring large intermediate results between systems.

Query pushdown leverages these performance efficiencies by enabling large and complex Spark logical plans (in parts) to be processed in TiKV.

Pushdown is not possible in all situations. For example, Spark UDFs cannot be pushed down to TiKV.

## Transaction support for Write
Since TiDB is a database that supports transaction, TiDB Spark Connector also support transaction, which means:
1. all data in DataFrame will be written to TiDB successfully, if no conflicts exist
2. no data in DataFrame will be written to TiDB successfully, if conflicts exist
3. no partial changes is visible to other session until commit.

## Upsert Semantics
TiSpark only support `Append` SaveMode.

`Append` in TiSpark means `upsert`,
1. if primary key exists in db, data will be updated
2. if no same primary key exists, data will be inserted.

| SaveMode | Support | Semantics |
| -------- | ------- | --------- |
| Append | true | TiSpark's `Append` means upsert. If primary key is same, data will be updated; if no same primary key exists, data will be inserted.  |
| Overwrite | false |  - |
| ErrorIfExists | false | - |
| Ignore | false | - |

Currently TiSpark only support writing data to such tables:
1. the table does not contain a primary key
2. the table's primary key is `TINYINT`、`SMALLINT`、`MEDIUMINT` or `INTEGER`
3. the table's primary key is `auto increment`

## Using the Spark Connector With Extensions Enabled
The connector adheres to the standard Spark API, but with the addition of TiDB-specific options.

The connector can be used both with or without extensions enabled. Here's examples about how to use it with extensions.

### init SparkConf
```scala
val sparkConf = new SparkConf()
  .setIfMissing("spark.master", "local[*]")
  .setIfMissing("spark.app.name", getClass.getName)
  .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
  .setIfMissing("spark.tispark.pd.addresses", "pd0:2379")
  .setIfMissing("spark.tispark.tidb.addr", "tidb")
  .setIfMissing("spark.tispark.tidb.password", "")
  .setIfMissing("spark.tispark.tidb.port", "4000")
  .setIfMissing("spark.tispark.tidb.user", "root")
 

val spark = SparkSession.builder.config(sparkConf).getOrCreate()
val sqlContext = spark.sqlContext
```

### Read using scala
```scala
// use tidb config in spark config if does not provide in data source config
val tidbOptions: Map[String, String] = Map()
val df = sqlContext.read
  .format("tidb")
  .options(tidbOptions)
  .option("database", "tpch_test")
  .option("table", "CUSTOMER")
  .load()
  .filter("C_CUSTKEY = 1")
  .select("C_NAME")
df.show()
```

### Write using scala
```scala
// use tidb config in spark config if does not provide in data source config
val tidbOptions: Map[String, String] = Map()

// data to write
val df = sqlContext.read
  .format("tidb")
  .options(tidbOptions)
  .option("database", "tpch_test")
  .option("table", "ORDERS")
  .load()

// Append
// if target_table_append does not exist, it will be created automatically
df.write
  .format("tidb")
  .options(tidbOptions)
  .option("database", "tpch_test")
  .option("table", "target_table_append")
  .mode("append")
  .save()
```

### Use another TiDB
TiDB config can be overwrite in data source options, thus one can connect to a different TiDB.

```scala
// tidb config priority: data source config > spark config
val tidbOptions: Map[String, String] = Map(
  "tidb.addr" -> "tidb",
  "tidb.password" -> "",
  "tidb.port" -> "4000",
  "tidb.user" -> "root",
  "spark.tispark.pd.addresses" -> "pd0:2379"
)

val df = sqlContext.read
  .format("tidb")
  .options(tidbOptions)
  .option("database", "tpch_test")
  .option("table", "CUSTOMER")
  .load()
  .filter("C_CUSTKEY = 1")
  .select("C_NAME")
df.show()
```

## Using the Spark Connector Without Extensions Enabled
Let's see how to use the connector without extensions enabled.

### init SparkConf
```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

val sparkConf = new SparkConf()
  .setIfMissing("spark.master", "local[*]")
  .setIfMissing("spark.app.name", getClass.getName)

val spark = SparkSession.builder.config(sparkConf).getOrCreate()
val sqlContext = spark.sqlContext
```

### Read using scala
```scala
// TiSpark's common options can also be passed in, 
// e.g. spark.tispark.plan.allow_agg_pushdown, spark.tispark.plan.allow_index_read, etc.
// spark.tispark.plan.allow_index_read is optional
val tidbOptions: Map[String, String] = Map(
  "tidb.addr" -> "tidb",
  "tidb.password" -> "",
  "tidb.port" -> "4000",
  "tidb.user" -> "root",
  "spark.tispark.pd.addresses" -> "pd0:2379"
)

val df = sqlContext.read
  .format("tidb")
  .options(tidbOptions)
  .option("database", "tpch_test")
  .option("table", "CUSTOMER")
  .load()
  .filter("C_CUSTKEY = 1")
  .select("C_NAME")
df.show()
```

## Write using scala
```scala
val tidbOptions: Map[String, String] = Map(
  "tidb.addr" -> "tidb",
  "tidb.password" -> "",
  "tidb.port" -> "4000",
  "tidb.user" -> "root",
  "spark.tispark.pd.addresses" -> "pd0:2379"
)

// data to write
val df = sqlContext.read
  .format("tidb")
  .options(tidbOptions)
  .option("database", "tpch_test")
  .option("table", "ORDERS")
  .load()

// Append
// if target_table_append does not exist, it will be created automatically
df.write
  .format("tidb")
  .options(tidbOptions)
  .option("database", "tpch_test")
  .option("table", "target_table_append")
  .mode("append")
  .save()
```

## TiDB Options
The following is TiDB-specific options, which can be passed in through `TiDBOptions` or `SparkConf`.

|    Key    | Short Name | Required | Description | Default |
| ---------- | --------- | -------- | ----------- | ------- |
| spark.tispark.pd.addresses | - | true | PD Cluster Addresses, split by comma | - |
| spark.tispark.tidb.addr | tidb.addr | true | TiDB Address, currently only support one instance | - |
| spark.tispark.tidb.port | tidb.port | true | TiDB Port | - |
| spark.tispark.tidb.user | tidb.user | true | TiDB User | - |
| spark.tispark.tidb.password | tidb.password | true | TiDB Password | - |
| database | - | true | TiDB Database | - |
| table | - | true | TiDB Table | - |
| deduplicate | - | false | Duplicate rows (same primary key) will be removed from DataFrame before writing to TiDB. Only one row with same primary key will be written successfully. | false |
| skipCommitSecondaryKey | - | false | skip commit secondary key | false |
| sampleFraction | - | false | sample fraction, from 0 to 1 | 0.01 |

TiSpark's common options can also be passed in, e.g. `spark.tispark.plan.allow_agg_pushdown`, `spark.tispark.plan.allow_index_read`, etc.