# Setting TiDB as metastore db

From time to time, users may need run multiple `spark-shell`s at same directory which often leads to some 
exceptions. Exceptions caused by lock conflicts: you already have a spark-shell running which blocks you run another spark-shell
at same directory. The way to address this need is setting tidb up as metastore db. 

## Setup TiDB

First you need a TiDB cluster(before 2.1 release), and then use a mysql client log into TiDB cluster. 

You will need to create a TiDB user for Spark to access the metastore.

```$xslt
CREATE USER 'hive'@'%' IDENTIFIED BY 'mine';
GRANT ALL PRIVILEGES ON metastore_db.* TO 'hive'@'%';
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

### Why only TiDB before 2.1 release works?

On Dec 10, 2018, a [PR](https://github.com/pingcap/tidb/pull/8625) got merged into TiDB's master.
The intention of this PR is to restrict the use of setting transaction isolation level such as serialize. 
After this change, setting transaction isolation level to serialize
will be an error rather than the noop in the past.

When hive initializes its metastore client; it will explicitly set transaction isolation level to 
serialize and cannot be adjusted by any configuration. This leads to restrict the specific version
of TiDB when you want to use tidb as a backend database to store metastore.

That is why we need choose a specific version of TiDB serving as an backend database to store
metastore.


## Adding hive-site.xml configuration to Spark

Then you can find a sample conf file [hive-site.xml.template](../config/hive-site.xml.template) and 
adjust some settings. You also need put the file into `SPARK_HOME/conf`.

After you finish these two steps, you are able to use tidb to store meta info of Spark.


