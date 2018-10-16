## TiSpark (version >= 2.0) on PySpark:
**Note: If you are using TiSpark version less than 2.0, please read [this document](./README_spark2.1.md) instead**
### Usage
There are currently three ways to use TiSpark on Python:
#### Directly via pyspark
This is the simplest way, just a decent Spark environment should be enough.
1. Make sure you have the latest version of [TiSpark](https://github.com/pingcap/tispark) and a `jar` with all TiSpark's dependencies.

2. Remember to add needed configurations listed in [README](../README.md) into your `$SPARK_HOME/conf/spark-defaults.conf`

3. Copy `./resources/session.py` to `$SPARK_HOME/python/pyspark/sql/session.py`

4. Run this command in your `$SPARK_HOME` directory:
```
./bin/pyspark --jars /where-ever-it-is/tispark-core-{$version}-jar-with-dependencies.jar
```

5. To use TiSpark, run these commands:
```python
# Query as you are in spark-shell
spark.sql("show databases").show()
spark.sql("use tpch_test")
spark.sql("show tables").show()
sql("select count(*) from customer").show()

# Result
# +--------+
# |count(1)|
# +--------+
# |     150|
# +--------+
```

#### Via spark-submit
This way is useful when you want to execute your own Python scripts.

Because of an open issue **[SPARK-25003]** in Spark 2.3, using spark-submit for python files will only support following api

1. Create a Python file named `test.py` as below:
```python
from py4j.java_gateway import java_import
from pyspark.context import SparkContext
 
# We get a referenct to py4j Java Gateway
gw = SparkContext._gateway

# Import TiExtensions
java_import(gw.jvm, "org.apache.spark.sql.TiExtensions")

# Inject TiExtensions, and get a TiContext
ti = gw.jvm.TiExtensions.getInstance(spark._jsparkSession).getOrCreateTiContext(spark._jsparkSession)

# Map database
ti.tidbMapDatabase("tpch_test", False, True)

# sql("use tpch_test")
sql("select count(*) from customer").show(20,200,False)
```

2. Prepare your TiSpark environment as above and execute
```bash
./bin/spark-submit --jars /where-ever-it-is/tispark-core-{$version}-jar-with-dependencies.jar test.py
```

3. Result:
```
+--------+
|count(1)|
+--------+
|     150|
+--------+
```


See [pytispark](https://pypi.python.org/pypi?:action=display&name=pytispark) for more information.