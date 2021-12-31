# Authorization and authentication through TiDB server

Authentication refers to the class of policies and mechanisms that verify that clients are allowed to connect as a
certain user.

Authorization is a process that occurs after authentication to determine what actions an account is permitted to
perform.

This feature allows you to execute SQL in TiSpark with Authorization and authentication, the same behavior as TiDB

## Prerequisites

- The database's user account must have the `PROCESS` privilege.
- TiSpark version >= 2.5.0
- Spark version = 3.0.x or 3.1.x

## Setup

Add the following lines in `spark-defaults.conf`.

```
spark.sql.tidb.addr    $your_tidb_server_address
spark.sql.tidb.port    $your_tidb_server_port
spark.sql.tidb.user    $your_tidb_server_user
spark.sql.tidb.password $your_tidb_server_password

// Must config in conf file
spark.sql.auth.enable   true
```

You can also config TiDB information in sparkSession

```scala
spark.sqlContext.setConf("spark.sql.tidb.addr", your_tidb_server_address)
spark.sqlContext.setConf("spark.sql.tidb.port", your_tidb_server_port)
spark.sqlContext.setConf("spark.sql.tidb.user", your_tidb_server_user)
spark.sqlContext.setConf("spark.sql.tidb.password", your_tidb_server_password)

```

## Configuration

|    Key    | Default Value | Description |
| ---------- | --- | --- |
| `spark.sql.tidb.auth.refreshInterval` |  `10` | The time interval for refreshing authorization information, in seconds. Values range from 5 to 3600 |

## Compatibility with catalog plugin

Currently, Supported statements are as follows:

|Statement   | Statement Support (Catalog Plugin)   | Statement Support | Authorization Support | 
|---|---|---|---|
| SELECT        | ✅ | ✅ | ✅ |   
| USE DATABASE  | ✅ | ✅ | ✅ |   
| SHOW DATABASES| ✅ | ✅ | ✅ |   
| SHOW TABLES   | ✅ | ✅ | ✅ |
| DESCRIBE TABLE| ✅ | ✅ | ✅ |
| SHOW COLUMNS  | ❌ | ✅ | ✅ |
| DESCRIBE COLUMN| ❌ | ✅ | ✅ |

## limitations

- Not supported with role-based privileges
- Not supported with TiDB Data Source API, such as TiBatchWrite