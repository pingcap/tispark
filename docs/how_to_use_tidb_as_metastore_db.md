# Setting TiDB as metastore db

From time to time, users may need run multiple `spark-shell`s at same directory which often leads to some 
exceptions. Exceptions caused by lock conflicts: you already have a spark-shell running which blocks you run another spark-shell
at same directory. The way to address this need is setting tidb up as metastore db. 

## Setup TiDB

First you need have a TiDB cluster present, and then use a mysql client log into TiDB cluster. 

You will need to create a TiDB user for Spark to access the metastore.

```$xslt
CREATE USER 'hive'@'%' IDENTIFIED BY 'mine';
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';
FLUSH PRIVILEGES;
```

It helps you create a user and grant access privileges to all databases and tables. 

## Adding hive-site.xml configuration to Spark

Then you can find a sample conf file [hive-site.xml.template](../config/hive-site.xml.template) and 
adjust some settings. You also need put the file into `SPARK_HOME/conf`.

After you finish these two steps, you are able to use tidb to store meta info of Spark.
