# Setting TiDB as metastore DB

Sometimes you might need to run many `spark-shell`s in the same directory, which often leads to exceptions.

Some exceptions are caused by lock conflicts. For example, you already have a `spark-shell` running which prevents you from running another `spark-shell` in same directory. The solution for this is to set TiDB up as the metastore DB.

## Setup TiDB

Create a TiDB `USER` with a password (for example, `hive` with password `mine`) for Spark to access the metastore.

```$xslt
CREATE USER 'hive'@'%' IDENTIFIED BY 'mine';
GRANT ALL PRIVILEGES ON metastore_db.* TO 'hive'@'%';
FLUSH PRIVILEGES;
```

The SQL statements above help you create a `USER` and grant `ACCESS` privileges to tables under `metastore_db`.

### Rely on Spark itself to initialize metastore

> **Warning:**
>
> It is **DANGEROUS** to rely on Spark itself to initialize metastore.
>
> It is **NOT** recommended to do so.

If you really need to do so, follow these steps to avoid unexpected schema corruption when code is changed:

 1. Make sure that there is no existing metastore and then use the official Spark schema tools for upgrade or migration.
 2. Fill in the root account in `hive-site.xml`. Let Spark use the root account to create metastore tables.
 3. Switch back to a normal account without any `CREATE` table and `ALTER` table privileges.

## Add `hive-site.xml` configuration to Spark

1. Find a sample configuration file [hive-site.xml.template](../config/hive-site.xml.template) and adjust some settings.
2. Put the file into `SPARK_HOME/conf`.
3. Use TiDB to store the meta information of Spark.
