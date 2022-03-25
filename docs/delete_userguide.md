# Delete feature

TiSpark delete feature provides the ability to delete bypass TiDB with spark SQL.

## Setup
To use delete in TiSpark, make sure you have configured Spark catalogs in `spark-defaults.conf`
```
spark.sql.catalog.tidb_catalog  org.apache.spark.sql.catalyst.catalog.TiCatalog
spark.sql.catalog.tidb_catalog.pd.addresses  ${your_pd_adress}
```

## Requirement
- TiDB 4.x or 5.x
- Spark >= 3.0

## Delete with SQL
```
spark.sql("delete from tidb_catalog.db.table where xxx")
```

## Limitation
- Delete without WHERE clause is not supported.
- Delete with subQuery is not supported.
- Delete from partition table is not supported.
- Delete with Pessimistic Transaction Mode is not supported.


