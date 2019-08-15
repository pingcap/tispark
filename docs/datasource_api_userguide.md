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
Since TiDB is a database that supports `transaction`, TiDB Spark Connector also supports `transaction`, which means:
1. all data in DataFrame will be written to TiDB successfully if no conflicts exist
2. no data in DataFrame will be written to TiDB successfully if conflicts exist
3. no partial changes are visible to other sessions until commit.

## Replace and insert semantics
TiSpark only supports `Append` SaveMode. The behavior is controlled by
`replace` option. The default value is false. In addition, if `replace` is true,
data to be inserted will be deduplicated before insertion.

If `replace` is true, then
* if the primary key or unique index exists in DB, data will be updated
* if no same primary key or unique index exists, data will be inserted.

If `replace` is false, then
* if the primary key or unique index exists in DB, data having conflicts expects an exception.
* if no same primary key or unique index exists, data will be inserted.

## Using the Spark Connector With Extensions Enabled
The connector adheres to the standard Spark API, but with the addition of TiDB-specific options.

The connector can be used both with or without extensions enabled. Here are examples about how to use it with extensions.

See [code examples with extensions](https://github.com/pingcap/tispark-test/blob/master/tispark-examples/src/main/scala/com/pingcap/tispark/examples/TiDataSourceExampleWithExtensions.scala).

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
/* create table before run the code
 CREATE TABLE tpch_test.target_table_orders (
   `O_ORDERKEY` int(11) NOT NULL,
   `O_CUSTKEY` int(11) NOT NULL,
   `O_ORDERSTATUS` char(1) NOT NULL,
   `O_TOTALPRICE` decimal(15,2) NOT NULL,
   `O_ORDERDATE` date NOT NULL,
   `O_ORDERPRIORITY` char(15) NOT NULL,
   `O_CLERK` char(15) NOT NULL,
   `O_SHIPPRIORITY` int(11) NOT NULL,
   `O_COMMENT` varchar(79) NOT NULL
 )
*/

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
 df.write
   .format("tidb")
   .options(tidbOptions)
   .option("database", "tpch_test")
   .option("table", "target_table_orders")
   .mode("append")
   .save()
```

### Use another TiDB
TiDB config can be overwrite in data source options, thus one can connect to a different TiDB.

```scala
// tidb config priority: data source config > spark config
val tidbOptions: Map[String, String] = Map(
  "tidb.addr" -> "anotherTidbIP",
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

See [code examples without extensions](https://github.com/pingcap/tispark-test/blob/master/tispark-examples/src/main/scala/com/pingcap/tispark/examples/TiDataSourceExampleWithoutExtensions.scala).

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
/* create table before run the code
 CREATE TABLE tpch_test.target_table_customer (
  `C_CUSTKEY` int(11) NOT NULL,
  `C_NAME` varchar(25) NOT NULL,
  `C_ADDRESS` varchar(40) NOT NULL,
  `C_NATIONKEY` int(11) NOT NULL,
  `C_PHONE` char(15) NOT NULL,
  `C_ACCTBAL` decimal(15,2) NOT NULL,
  `C_MKTSEGMENT` char(10) NOT NULL,
  `C_COMMENT` varchar(117) NOT NULL
)
*/

// Common options can also be passed in,
// e.g. spark.tispark.plan.allow_agg_pushdown, spark.tispark.plan.allow_index_read, etc.
// spark.tispark.plan.allow_index_read is optional
val tidbOptions: Map[String, String] = Map(
  "tidb.addr" -> "127.0.0.1",
  "tidb.password" -> "",
  "tidb.port" -> "4000",
  "tidb.user" -> "root",
  "spark.tispark.pd.addresses" -> "127.0.0.1:2379"
)

val df = readUsingScala(sqlContext)

// Append
df.write
  .format("tidb")
  .options(tidbOptions)
  .option("database", "tpch_test")
  .option("table", "target_table_customer")
  .mode("append")
  .save()
```

## Use Data Source API in SparkSQL
Config tidb/pd address and enable write through SparkSQL in `conf/spark-defaults.conf` as follows:

```
spark.tispark.pd.addresses 127.0.0.1:2379
spark.tispark.tidb.addr 127.0.0.1
spark.tispark.tidb.port 4000
spark.tispark.tidb.user root
spark.tispark.tidb.password password
spark.tispark.write.allow_spark_sql true
```

create a new table using mysql-client:
```
CREATE TABLE tpch_test.TARGET_TABLE_CUSTOMER (
  `C_CUSTKEY` int(11) NOT NULL,
  `C_NAME` varchar(25) NOT NULL,
  `C_ADDRESS` varchar(40) NOT NULL,
  `C_NATIONKEY` int(11) NOT NULL,
  `C_PHONE` char(15) NOT NULL,
  `C_ACCTBAL` decimal(15,2) NOT NULL,
  `C_MKTSEGMENT` char(10) NOT NULL,
  `C_COMMENT` varchar(117) NOT NULL
)
```

register a tidb table `tpch_test.CUSTOMER` to spark catalog:
```
CREATE TABLE CUSTOMER_SRC USING tidb OPTIONS (database 'tpch_test', table 'CUSTOMER')
```

select data from `tpch_test.CUSTOMER`:
```
SELECT * FROM CUSTOMER_SRC limit 10
```

register another tidb table `tpch_test.TARGET_TABLE_CUSTOMER` to spark catalog:
```
CREATE TABLE CUSTOMER_DST USING tidb OPTIONS (database 'tpch_test', table 'TARGET_TABLE_CUSTOMER')
```

write data to `tpch_test.TARGET_TABLE_CUSTOMER`:
```
INSERT INTO CUSTOMER_DST VALUES(1000, 'Customer#000001000', 'AnJ5lxtLjioClr2khl9pb8NLxG2', 9, '19-407-425-2584', 2209.81, 'AUTOMOBILE', '. even, express theodolites upo')

INSERT INTO CUSTOMER_DST SELECT * FROM CUSTOMER_SRC
```

## TiDB Options
The following is TiDB-specific options, which can be passed in through `TiDBOptions` or `SparkConf`.

| Key                         | Short Name    | Required | Description                                                                                                 | Default |
| --------------------------- | ------------- | -------- | ----------------------------------------------------------------------------------------------------------- | ------- |
| spark.tispark.pd.addresses  | -             | true     | PD Cluster Addresses, split by comma                                                                        | -       |
| spark.tispark.tidb.addr     | tidb.addr     | true     | TiDB Address, currently only support one instance                                                           | -       |
| spark.tispark.tidb.port     | tidb.port     | true     | TiDB Port                                                                                                   | -       |
| spark.tispark.tidb.user     | tidb.user     | true     | TiDB User                                                                                                   | -       |
| spark.tispark.tidb.password | tidb.password | true     | TiDB Password                                                                                               | -       |
| database                    | -             | true     | TiDB Database                                                                                               | -       |
| table                       | -             | true     | TiDB Table                                                                                                  | -       |
| skipCommitSecondaryKey      | -             | false    | skip commit secondary key                                                                                   | false   |
| enableRegionSplit           | -             | false    | do region split to avoid hot region during insertion                                                        | true    |
| regionSplitNum              | -             | false    | user defined region split number during insertion                                                           | 0       |
| replace                     | -             | false    | define the behavior of append.                                                                              | false   |
| lockTTLSeconds              | -             | false    | tikv lock ttl, write duration must < `lockTTLSeconds`, otherwise write may fail because of gc               | 3600    |
| writeConcurrency            | -             | false    | maximum number of threads writing data to tikv, suggest `writeConcurrency` <= 8 * `number of TiKV instance` | 0       |

## TiDB Version and Configuration for Write
TiDB's version should >= 3.0 and make sure that the following tidb's configs are correctly set.

```
# enable-table-lock is used to control table lock feature. The default value is false, indicating the table lock feature is disabled.
enable-table-lock: true

# delay-clean-table-lock is used to control the time (Milliseconds) of delay before unlock the table in the abnormal situation.
delay-clean-table-lock: 60000

# When create table, split a separated region for it. It is recommended to
# turn off this option if there will be a large number of tables created.
split-table: true
```

If your TiDB's version is < 3.0 and want to have a try, you can set `spark.tispark.write.without_lock_table` to `true` to enable write, but no ACID is guaranteed.

## Type Conversion for Write
The following SparkSQL Data Type is currently not supported for writing to TiDB:
- BinaryType
- ArrayType
- MapType
- StructType

The full conversion metrics is as follows.


| Write      | Boolean            | Byte               | Short              | Integer            | Long               | Float              | Double             | String             | Decimal            | Date               | Timestamp          |
| ---------- | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ |
| BIT        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| BOOLEAN    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                |
| TINYINT    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                |
| SMALLINT   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                |
| MEDIUMINT  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                |
| INTEGER    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                |
| BIGINT     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                |
| FLOAT      | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| DOUBLE     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| DECIMAL    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                |
| DATE       | :x:                | :x:                | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :white_check_mark: | :white_check_mark: |
| DATETIME   | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :white_check_mark: | :x:                | :white_check_mark: | :white_check_mark: |
| TIMESTAMP  | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :white_check_mark: | :x:                | :white_check_mark: | :white_check_mark: |
| TIME       | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| YEAR       | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| CHAR       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| VARCHAR    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| TINYTEXT   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| TEXT       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| MEDIUMTEXT | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| LONGTEXT   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| BINARY     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :x:                | :x:                | :x:                |
| VARBINARY  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :x:                | :x:                | :x:                |
| TINYBLOB   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :x:                | :x:                | :x:                |
| BLOB       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :x:                | :x:                | :x:                |
| MEDIUMBLOB | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :x:                | :x:                | :x:                |
| LONGBLOB   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :x:                | :x:                | :x:                |
| ENUM       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                |
| SET        | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
