# Setting TiDB as metastore db

Generally sparking, you need setup tidb first and then copy `hive-site.xml` into Spark's `conf`
directory.

## Setup TiDB

First you need have a TiDB cluster present, and then use a mysql client log into TiDB cluster. 

You will need execute the following command:

```$xslt
CREATE USER 'hive'@'%' IDENTIFIED BY 'mine';
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';
FLUSH PRIVILEGES;
```

It helps you create a user and grant access privileges to all databases and tables. 

## Adding hive-site.xml configuration to Spark

Then you can find a sample conf file at config directory at [tispark](https://github.com/pingcap/tispark)
project. What you need to do is copy and paste such file into `SPARK_HOME/conf`. One last thing to mention 
is that you need to adjust some properties if you change the user name or user password at 
setting up tidb step. 

After you finish these two steps, you are able to use tidb to store meta info of Spark.
