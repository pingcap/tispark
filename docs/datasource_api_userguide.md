# TiDB Data Source API User Guide

The TiDB Connector for Spark enables you to use TiDB as the data source of Apache Spark, similar to other data sources (PostgreSQL, HDFS, S3, etc.).

The TiDB Connector supports Spark 2.3.0+ and Spark 2.4.0+.

## Interaction Between TiDB and Spark

TiDB Connector supports bi-directional data movement between TiDB and Spark clusters.

Using the connector, you can perform the following two operations:

  - Populate a Spark DataFrame from a table in TiDB.
  - Write the contents of a Spark DataFrame to a table in TiDB.

## Transaction support for Write

Since TiDB is a database that supports `transaction`, the TiDB Connector for Spark also supports `transaction`, which means that:

1. all the data in DataFrame is written to TiDB successfully if no conflicts exist.
2. no data in DataFrame is written to TiDB successfully if conflicts exist.
3. no partial changes are visible to other sessions until the transaction is committed.

## `REPLACE` and `INSERT` syntaxes

TiSpark only supports the `Append` SaveMode. This behavior is controlled by the `replace` option. The default value is `false`.

In addition, if `replace` is true, data to be inserted is duplicated before the insertion.

+ If `replace` is `true`:

    - if the primary key or unique index exists in the table, data is updated.
    - if no same primary key or unique index exists, data is inserted.

+ If `replace` is `false`:

    - if the primary key or unique index exists in the database, data with conflicts expects an exception.
    - if no same primary key or unique index exists, data is inserted.

## Use Spark connector with extensions enabled

The Spark connector adheres to the standard Spark API, but with the addition of TiDB-specific options.

The connector can be used either with or without extensions enabled.

Fow how to use it with extensions enabled, see [code examples with extensions](https://github.com/pingcap/tispark-test/blob/master/tispark-examples/src/main/scala/com/pingcap/tispark/examples/TiDataSourceExampleWithExtensions.scala).

1. Initiate `SparkConf`.

    ```scala
    val sparkConf = new SparkConf().
      setIfMissing("spark.master", "local[*]").
      setIfMissing("spark.app.name", getClass.getName).
      setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions").
      setIfMissing("spark.tispark.pd.addresses", "pd0:2379").
      setIfMissing("spark.tispark.tidb.addr", "tidb").
      setIfMissing("spark.tispark.tidb.port", "4000")
      // if tidb < 3.0.14, please set the spark.tispark.write.without_lock_table=true
      // .setIfMissing("spark.tispark.write.without_lock_table", "true")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    ```

2. Read using scala.

    ```scala
    val sqlContext = spark.sqlContext

    // use TiDB config in the spark config if no data source config is provided
    val tidbOptions: Map[String, String] = Map(
      "tidb.user" -> "root",
      "tidb.password" -> ""
    )

    val df = sqlContext.read.
      format("tidb").
      options(tidbOptions).
      option("database", "tpch_test").
      option("table", "CUSTOMER").
      load().
      filter("C_CUSTKEY = 1").
      select("C_NAME")
    df.show()
    ```

3. Write using scala.

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

    // use TiDB config in the spark config if no data source config is provided
    val tidbOptions: Map[String, String] = Map(
      "tidb.user" -> "root",
      "tidb.password" -> ""
    )

    // data to write
    val df = sqlContext.read.
      format("tidb").
      options(tidbOptions).
      option("database", "tpch_test").
      option("table", "ORDERS").
      load()

    // append
    df.write.
      format("tidb").
      options(tidbOptions).
      option("database", "tpch_test").
      option("table", "target_table_orders").
      mode("append").
      save()
    ```

4. Use another TiDB.

   TiDB configuration can be overwritten in data source options, so you can connect to a different TiDB.

    ```scala
    // TiDB config priority: data source config > spark config
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "anotherTidbIP",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "pd0:2379"
    )

    val df = sqlContext.read.
      format("tidb").
      options(tidbOptions).
      option("database", "tpch_test").
      option("table", "CUSTOMER").
      load().
      filter("C_CUSTKEY = 1").
      select("C_NAME").
    df.show()
    ```

## Use Data Source API in SparkSQL

1. Configure TiDB or PD addresses and enable write through SparkSQL in `conf/spark-defaults.conf` as follows:

```
spark.tispark.pd.addresses 127.0.0.1:2379
spark.tispark.tidb.addr 127.0.0.1
spark.tispark.tidb.port 4000
spark.tispark.write.allow_spark_sql true
# if tidb <= 3.0.14, please set the spark.tispark.write.without_lock_table=true
# spark.tispark.write.without_lock_table true
```

2. Create a new table using mysql-client:

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

3. Register a TiDB table `tpch_test.CUSTOMER` to the Spark Catalog:

```
CREATE TABLE CUSTOMER_SRC USING tidb OPTIONS (
  tidb.user 'root',
  tidb.password '',
  database 'tpch_test',
  table 'CUSTOMER'
)
```

4. Select data from `tpch_test.CUSTOMER`:

```
SELECT * FROM CUSTOMER_SRC limit 10
```

5. Register another TiDB table `tpch_test.TARGET_TABLE_CUSTOMER` to the Spark Catalog:

```
CREATE TABLE CUSTOMER_DST USING tidb OPTIONS (database 'tpch_test', table 'TARGET_TABLE_CUSTOMER')
```

6. Write data to `tpch_test.TARGET_TABLE_CUSTOMER`:

```
INSERT INTO CUSTOMER_DST VALUES(1000, 'Customer#000001000', 'AnJ5lxtLjioClr2khl9pb8NLxG2', 9, '19-407-425-2584', 2209.81, 'AUTOMOBILE', '. even, express theodolites upo')

INSERT INTO CUSTOMER_DST SELECT * FROM CUSTOMER_SRC
```

## TiDB Options

The following table shows the TiDB-specific options, which can be passed in through `TiDBOptions` or `SparkConf`.

| Key                        | Short Name    | Required | Default | Description                                                                                                                                    |
| -------------------------- | ------------- | -------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| spark.tispark.pd.addresses | -             | true     | -       | The addresses of PD clusters, split by comma                                                                                                   |
| spark.tispark.tidb.addr    | tidb.addr     | true     | -       | TiDB address, which currently only supports one instance                                                                                       |
| spark.tispark.tidb.port    | tidb.port     | true     | -       | TiDB Port                                                                                                                                      |
| spark.tispark.tidb.user    | tidb.user     | true     | -       | TiDB User                                                                                                                                      |
| tidb.password              | tidb.password | true     | -       | TiDB Password                                                                                                                                  |
| database                   | -             | true     | -       | TiDB Database                                                                                                                                  |
| table                      | -             | true     | -       | TiDB Table                                                                                                                                     |
| replace                    | -             | false    | false   | To define the behavior of append                                                                                                               |
| skipCommitSecondaryKey     | -             | false    | false   | Whether to skip the commit phase of secondary keys                                                                                             |
| enableRegionSplit          | -             | false    | true    | To split Region to avoid hot Region during insertion                                                                                           |
| regionSplitNum             | -             | false    | 0       | The Region split number defined by user during insertion                                                                                       |
| writeConcurrency           | -             | false    | 0       | The maximum number of threads that write data to TiKV. It is recommended that `writeConcurrency` is smaller than 8 * `number of TiKV instance` |
| snapshotBatchGetSize       | -             | false    | 2048    | The max size of keys for calling `Snapshot.batchGet`                                                                                               |

## TiDB Version and Configuration for Write

TiDB's version must be 3.0.14 or later.

Make sure that the following TiDB configuration items are correctly set.

```
# enable-table-lock is used to control the table lock feature. The default value is false, indicating that the table lock feature is disabled.
enable-table-lock: true

# delay-clean-table-lock is used to control the time (milliseconds) of delay before unlocking the table in abnormal situations.
delay-clean-table-lock: 60000

# When creating table, split a separated Region for it. It is recommended to
# turn off this option if there is a large number of tables created.
split-table: true
```

If your TiDB's version is earlier than 3.0.14, set `spark.tispark.write.without_lock_table` to `true` to enable write, but ACID is **not** guaranteed.

### WARNING
**DO NOT set `spark.tispark.write.without_lock_table` to `true` on production environment (you may lost data).**

## Type Conversion for Write

The following types of SparkSQL Data are currently not supported for writing to TiDB:

- BinaryType
- ArrayType
- MapType
- StructType

The complete conversion metrics are as follows.

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

## Write Benchmark

The following test report is based on 4 machines.

```
Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz * 2 = 40Vu
12 * 16G = 188G
```

`FIO` test result:

```
WRITE: bw=705MiB/s (740MB/s), 705MiB/s-705MiB/s (740MB/s-740MB/s), io=20.0GiB (21.5GB), run=29034-29034msec
```

The table schema is:

```
CREATE TABLE ORDERS  (O_ORDERKEY       INTEGER NOT NULL,
                      O_CUSTKEY        INTEGER NOT NULL,
                      O_ORDERSTATUS    CHAR(1) NOT NULL,
                      O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                      O_ORDERDATE      DATE NOT NULL,
                      O_ORDERPRIORITY  CHAR(15) NOT NULL,
                      O_CLERK          CHAR(15) NOT NULL,
                      O_SHIPPRIORITY   INTEGER NOT NULL,
                      O_COMMENT        VARCHAR(79) NOT NULL);

```

### TiSpark write benchmark

| Count(*)    | Data Size | Parallel Number | Prepare(s) | Pre-write(s) | Commit(s) | Total(s) |
| ----------- | --------- | --------------- | ---------- | ------------ | ---------- | --------- |
| 1,500,000   | 165M      | 2               | 17         | 68           | 62         | 148       |
| 15,000,000  | 1.7G      | 24              | 49         | 157          | 119        | 326       |
| 150,000,000 | 17G       | 120             | 630        | 1236         | 1098       | 2964      |

## Spark with JDBC Benchmark

| Count(*)    | Data Size | Parallel Number | Spark JDBC Write(s) | Comments                            |
| ----------- | --------- | --------------- | -------------------- | ----------------------------------- |
| 1,500,000   | 165M      | 24              | 22                   |                                     |
| 15,000,000  | 1.7G      | 24              | 411                  | Using 120 parallel causes `KV Busy`. |
| 150,000,000 | 17G       | 24              | 2936                 | Using 120 parallel causes `KV Busy`. |
