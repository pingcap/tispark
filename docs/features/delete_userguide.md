# Delete feature

TiSpark delete feature provides the ability to delete bypass TiDB with spark SQL.

## Setup
To use delete in TiSpark, make sure you have configured Spark catalogs in `spark-defaults.conf`
```
spark.sql.catalog.tidb_catalog  org.apache.spark.sql.catalyst.catalog.TiCatalog
spark.sql.catalog.tidb_catalog.pd.addresses  ${your_pd_address}
```

## Requirement
- TiDB 4.x or 5.x
- Spark = 3.0.x or 3.1.x or 3.2.x or 3.3.x

## Delete with SQL
```
spark.sql("delete from tidb_catalog.db.table where xxx")
```
You can also customize some options 
```
spark.conf.set("writeThreadPerTask","3")
spark.sql("delete from tidb_catalog.db.table where xxx")
```


## Configuration

| Key                   | Default | Description                                             |
| --------------------- | ------- | ------------------------------------------------------- |
| writeThreadPerTask    | 2       | Thread number each spark task use to write data to TiKV |
| prewriteMaxRetryTimes | 64      | Max retry times for prewrite                            |


## Limitation
- Delete without WHERE clause is not supported.
- Delete with subQuery is not supported.
- Delete from partition table is not supported.
- Delete with Pessimistic Transaction Mode is not supported.

