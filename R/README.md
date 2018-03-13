## TiSparkR
A thin layer build for supporting R language with TiSpark

### Usage
1. Download TiSparkR source code and build a binary package(run `R CMD build R` in TiSpark root directory). Install it to your local R library(e.g. via `R CMD INSTALL TiSparkR_1.0.0.tar.gz`)
2. Build or download TiSpark dependency jar `tispark-core-1.0-RC1-jar-with-dependencies.jar` [here](https://github.com/pingcap/tispark).
3. `cd` to your Spark home directory, and run
```
./bin/sparkR --jars /where-ever-it-is/tispark-core-1.0-RC1-jar-with-dependencies.jar
```
Note that you should replace the `TiSpark` jar path with your own.
 
4. Use as below in your R console:
```R
# import tisparkR library
> library(TiSparkR)
# create a TiContext instance
> ti <- createTiContext(spark)
# Map TiContext to database:tpch_test
> tidbMapDatabase(ti, "tpch_test")

# Run a sql query
> customers <- sql("select * from customer")
# Print schema
> printSchema(customers)
root
 |-- c_custkey: long (nullable = true)
 |-- c_name: string (nullable = true)
 |-- c_address: string (nullable = true)
 |-- c_nationkey: long (nullable = true)
 |-- c_phone: string (nullable = true)
 |-- c_acctbal: decimal(15,2) (nullable = true)
 |-- c_mktsegment: string (nullable = true)
 |-- c_comment: string (nullable = true)
 
# Run a count query
> count <- sql("select count(*) from customer")
# Print count result
> head(count)
  count(1)
1      150
```