## TiSparkR

### Usage
There are currently two ways to use TiSpark on SparkR:

#### Directly via sparkR
This is the simplest way, just a decent Spark environment should be enough.
1. Make sure you have the latest version of [TiSpark](https://github.com/pingcap/tispark) and a `jar` with all TiSpark's dependencies.

2. Remember to add needed configurations listed in [README](../README.md) into your `$SPARK_HOME/conf/spark-defaults.conf`

3. Run this command in your `$SPARK_HOME` directory:
```
./bin/sparkR --jars /where-ever-it-is/tispark-${name_with_version}.jar
```

4. To use TiSpark, run these commands:
```R
sql("use tpch_test")
count <- sql("select count(*) from customer")
head(count)
```

#### Via spark-submit
This way is useful when you want to execute your own R scripts.

1. Create a R file named `test.R` as below:
```R
library(SparkR)
sparkR.session()
sql("use tpch_test")
count <- sql("select count(*) from customer")
head(count)
```

2. Prepare your TiSpark environment as above and execute
```bash
./bin/spark-submit --jars /where-ever-it-is/tispark-${name_with_version}.jar test.R
```

3. Result:
```
+--------+
|count(1)|
+--------+
|     150|
+--------+
```