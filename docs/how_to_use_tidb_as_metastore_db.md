# Setting TiDB as metastore db

From time to time, users may need run multiple `spark-shell`s at same directory which often leads to some 
exceptions. Exceptions caused by lock conflicts: you already have a spark-shell running which blocks you run another spark-shell
at same directory. The way to address this need is setting tidb up as metastore db. 

## Setup TiDB

First you need a TiDB cluster, and then use a mysql client log into TiDB cluster. 

You will need to create a TiDB user for Spark to access the metastore.

```$xslt
CREATE USER 'hive'@'%' IDENTIFIED BY 'mine';
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';
FLUSH PRIVILEGES;
```

It helps you create a user and grant access privileges to all databases and tables. Something need to
be noted is:

> This is actually very dangerous and not recommended. If you rely on spark itself to initialize metastore, 
please do following:
> 1. Make sure there is no existing metastore. If so, please use official spark schema tools to upgrade or migrate.
> 2. Fill in root account in hive-site.xml. Let spark use root account to create metastore tables.
> 3. Then switch back to a normal account without any create table and alter table privileges.
>
> This preventing unexpected schema corruption when code changes.

## Adding hive-site.xml configuration to Spark

Then you can find a sample conf file [hive-site.xml.template](../config/hive-site.xml.template) and 
adjust some settings. You also need put the file into `SPARK_HOME/conf`.

After you finish these two steps, you are able to use tidb to store meta info of Spark.
