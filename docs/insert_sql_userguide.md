# TiSpark Insert SQL User Guide

TiSpark enables you to use TiDB as the data source of Apache Spark, similar to other data sources (PostgreSQL, HDFS, S3, etc.).

It supports insert data using Spark SQL now.

## Use Spark SQL
1. Initiate `SparkConf`.

    ```scala
    val sparkConf = new SparkConf().
      setIfMissing("spark.master", "local[*]").
      setIfMissing("spark.app.name", getClass.getName).
      setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions").
      setIfMissing("spark.tispark.pd.addresses", "pd0:2379").
      setIfMissing("spark.tispark.tidb.addr", "tidb").
      setIfMissing("spark.tispark.tidb.port", "4000")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    ```

2. Write using Spark SQL.

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
   
   //use catalog
   spark.sql("use tidb_catalog")
    
   // write data to tidb
    spark.sql(
          "insert into tpch_test.target_table_orders values" +
            "(1,1,'0',11.11,date'2022-1-1','first','spark',12345,'no comment')")

    // select data
    spark.sql("select * from tpch_test.target_table_orders").show
   ```

## TiDB Options

The following table shows the TiDB-specific options, which can be passed in through `SparkConf` or `spark.conf.set`.
But you should not set `tidb.password` in `SparkConf` and only use `spark.conf.set` to set it. 

| Key                         | Required | Default         | Description                                                                                                                   |
|-----------------------------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------|
| tidb.addr                   | false    | -               | TiDB address, needed when enableUpdateTableStatistics is true                                                                 |
| tidb.port                   | false    | -               | TiDB Port, needed when enableUpdateTableStatistics is true                                                                    |
| tidb.user                   | false    | -               | TiDB User, needed when enableUpdateTableStatistics is true                                                                    |
| tidb.password               | false    | -               | TiDB Password, needed when enableUpdateTableStatistics is true                                                                |
| enableRegionSplit           | false    | true            | To split Region to avoid hot Region during insertion                                                                          |
| scatterWaitMS               | false    | 300000          | Max time to wait scatter region                                                                                               |
| writeThreadPerTask          | false    | 1               | Thread number each spark task use to write data to TiKV                                                                       |
| bytesPerRegion              | false    | 100663296 (96M) | Decrease this parameter to split more regions (increase write concurrency)                                                    |
| enableUpdateTableStatistics | false    | false           | Update statistics in table `mysql.stats_meta` (`tidb.user` must own update privilege to table `mysql.stats_meta` if set true) |
| rowFormatVersion            | false    | 2               | Version of row format to save data                                                                                            | 

## TiDB Version

TiDB's version must be **4.0.0 or later**.

## Limitations
TiSpark support write into partition table using INSERT SQL now.

But TiSpark does not support insert with a partition spec and a column list, like:

`INSERT INTO test.test PARTITION (i = 1)(s, k) VALUES ('hello', 'world');`

## Type Conversion for Write

The following types of SparkSQL Data are currently not supported for writing to TiDB:

- ArrayType
- MapType
- StructType

Spark SQL and TIDB have different conversion rules. We follow this [rule](https://github.com/pingcap/tispark/blob/master/docs/datasource_api_userguide.md#type-conversion-for-write) when using DataSource API,
but follow the rule of Spark SQL when using insert.

There is some CAST rules supported by Spark which must be explicit indicated. Like
`SELECT CAST('2020-01-01' AS Timestamp)`. [Here](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)
for more details.

When convert Float/Double to Int/Short/Long, there is a subtle different between them. TiDB will use
`round(num)`, Spark SQL will use `floor(num)`.

The complete conversion metrics are as follows.

| target\source | Boolean            | Binary             | Short              | Integer            | Long               | Float              | Double             | String             | Decimal            | Date               | Timestamp          |
|---------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------| ------------------ |--------------------| ------------------ |
| BIT           | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| BOOLEAN       | :white_check_mark: | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| TINYINT       | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| SMALLINT      | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| MEDIUMINT     | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| INTEGER       | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| BIGINT        | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| FLOAT         | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| DOUBLE        | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                |
| DECIMAL       | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :white_check_mark: | :x:                | :x:                |
| DATE          | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :white_check_mark: | :white_check_mark: |
| DATETIME      | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :white_check_mark: | :white_check_mark: |
| TIMESTAMP     | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :white_check_mark: | :white_check_mark: |
| TIME          | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| YEAR          | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| CHAR          | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| VARCHAR       | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| TINYTEXT      | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| TEXT          | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| MEDIUMTEXT    | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| LONGTEXT      | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| BINARY        | :x:                | :white_check_mark: | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| VARBINARY     | :x:                | :white_check_mark: | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| TINYBLOB      | :x:                | :white_check_mark: | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| BLOB          | :x:                | :white_check_mark: | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| MEDIUMBLOB    | :x:                | :white_check_mark: | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| LONGBLOB      | :x:                | :white_check_mark: | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| ENUM          | :x:                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                |
| SET           | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |
| JSON          | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                | :x:                |