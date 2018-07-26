## TiSpark on PySpark:
### Usage
There are currently three ways to use TiSpark on Python:
#### Directly via pyspark
This is the simplest way, just a decent Spark environment should be enough.
1. Make sure you have the latest version of [TiSpark](https://github.com/pingcap/tispark) and a `jar` with all TiSpark's dependencies.

2. Run this command in your `SPARK_HOME` directory:
```
./bin/pyspark --jars /where-ever-it-is/tispark-${version}-jar-with-dependencies.jar
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
ti.tidbMapDatabase("tpch_test", False, True)
 
# Query as usual
sql("select count(*) from customer").show()
 
# Result
# +--------+
# |count(1)|
# +--------+
# |     150|
# +--------+
```
#### Via pip
This way is generally the same as the first way, but more readable.
1. Use ```pip install pytispark``` in your console to install `pytispark`  

2. Make sure you have the latest version of [TiSpark](https://github.com/pingcap/tispark) and a `jar` with all TiSpark's dependencies.

3. Run this command in your `SPARK_HOME` directory:
```
./bin/pyspark --jars /where-ever-it-is/tispark-core-${version}-jar-with-dependencies.jar
```

4. Use as below:
```python
import pytispark.pytispark as pti
 
ti = pti.TiContext(spark)
 
ti.tidbMapDatabase("tpch")
 
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
1. Create a Python file named `test.py` as below:
```python
from pyspark.sql import SparkSession
import pytispark.pytispark as pti

spark = SparkSession.builder.master("Your master url").appName("Your app name").getOrCreate()

ti = pti.TiContext(spark)
 
ti.tidbMapDatabase("tpch")
 
spark.sql("select count(*) from customer").show()
```

2. Prepare your TiSpark environment as above and execute
```bash
./bin/spark-submit --jars /where-ever-it-is/tispark-core-1.0.1-jar-with-dependencies.jar test.py
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