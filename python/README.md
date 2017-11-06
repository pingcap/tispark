## TiSpark on PySpark:
### Usage
1. Make sure you have the latest version of [TiSpark](https://github.com/pingcap/tispark) and a `jar` with all TiSpark's dependencies.

2. Run this command in your `SPARK_HOME` directory:
```
./bin/pyspark --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

3. To use TiSpark, run these commands:
```python
# First we need to import py4j java_import module
# Along with spark
from py4j.java_gateway import java_import
from pyspark.context import SparkContext
 
# We get a referenct to py4j Java Gateway
gw = SparkContext._gateway
 
java_import(gw.jvm, "org.apache.spark.sql.TiContext")
 
# Create a TiContext
ti = gw.jvm.TiContext(spark._jsparkSession)
 
# Map database
ti.tidbMapDatabase("tpch_test", False)
 
# Query as usual
sql("select count(*) from customer").show()
 
# Result
# +--------+
# |count(1)|
# +--------+
# |     150|
# +--------+
```
