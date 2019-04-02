## TiSpark (version >= 2.0) on PySpark:
**Note: If you are using TiSpark version less than 2.0, please read [this document](./README_spark2.1.md) instead**

pytispark will not be necessary since TiSpark version >= 2.0.
### Usage
There are currently two ways to use TiSpark on Python:
#### Directly via pyspark
This is the simplest way, just a decent Spark environment should be enough.
1. Make sure you have the latest version of [TiSpark](https://github.com/pingcap/tispark) and a `jar` with all TiSpark's dependencies.

2. Remember to add needed configurations listed in [README](../README.md) into your `$SPARK_HOME/conf/spark-defaults.conf`

3. Copy `./resources/session.py` to `$SPARK_HOME/python/pyspark/sql/session.py`

4. Run this command in your `$SPARK_HOME` directory:
```
./bin/pyspark --jars /where-ever-it-is/tispark-core-${version}-jar-with-dependencies.jar
```

5. To use TiSpark, run these commands:
```python
# Query as you are in spark-shell
spark.sql("show databases").show()
spark.sql("use tpch_test")
spark.sql("show tables").show()
spark.sql("select count(*) from customer").show()

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

1. Use ```pip install pytispark``` in your console to install `pytispark` 

Note that you may need reinstall `pytispark` if you meet `No plan for reation` error.

2. Create a Python file named `test.py` as below:
```python
import pytispark.pytispark as pti
from pyspark.sql import SparkSession
spark = SparkSession.getOrCreate()
ti = pti.TiContext(spark)

ti.tidbMapDatabase("tpch_test")

spark.sql("select count(*) from customer").show()

# Result
# +--------+
# |count(1)|
# +--------+
# |     150|
# +--------+
```

3. Prepare your TiSpark environment as above and execute
```bash
./bin/spark-submit --jars /where-ever-it-is/tispark-core-${version}-jar-with-dependencies.jar test.py
```

4. Result:
```
+--------+
|count(1)|
+--------+
|     150|
+--------+
```


See [pytispark](https://pypi.python.org/pypi?:action=display&name=pytispark) for more information.