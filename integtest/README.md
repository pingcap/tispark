## How to do Integration Test

It requires both TiDB and Spark be present. The test framework will run queries in both TiDB and Spark and compare their results.

A user might attach to an existing database with existing data for testing or dump test data and schema via this tool to create testcases as well.

You may edit `properties.template` files according to your current mode

Test cases will be searched recursively. 

#### File Extension Introduction

    *.ddl   include SQL statements of test table schema; 
    *.sql   include SQL test statements;
    *.data  include test table data.

#### How to start an integration test
run `dump.sh` to create test data for databases listed in `dbNameList`:
```
./dump.sh dbNameList
```
`dbNameList` is separated by comma, e.g. `db1,db2`

**Note: By doing this you will overwrite data stored in db1.data and db2.data with data stored in your local database.**

Load test data from `*.data` recursively from directory:
```
./load.sh 
```
Run Test on existing data: (use -d or --debug option to setup debug flag)
```
./test.sh [-d|--debug]
```
To run tpch Test individually: (use -d or --debug option to setup debug flag)
```
./test_tpch.sh [-d|--debug]
```
To run index Test individually: (use -h option for help)
```
./test_index.sh [-d | -a | -h | -s | -i | -r]
```

In general, use `-r` flag to show result only when you run index Test

*To Build up tpch test files, please follow the instructions in `test_tpch.sh`*

In case debug flag is set, JVM remote debug port will open at 5005.

You might also run tests manually for other use cases:
```
java -Dtest.mode=Load -cp ./conf:$./lib/* com.pingcap.spark.TestFramework
```

#### Demo: Adding a new test case

Before you add a new test case, you may first dump your database from storage containing test data.

```
./dump.sh db1[,db2[,...]]
```

Or you can make your own data with `*.ddl` and `*.data` files, check `t1.data` and `t1.ddl` in directory `./testcase/test_index/` for 
brief example. Remember if you create data in this way, its parent directory name would define the database containing test data. In the 
case above, table `t1` would be created in database `test_index`.

Now you can load your data by executing

```
./load.sh
```

Write your own sql statement in `.sql` file and put them in the same directory with local data. Statement format should be accepted by both spark and tidb in order it works. See `./testcase/test_index/t1.sql` for example.

Run `./test.sh` and start integration test containing your own test cases.


#### Configuration
Here is a sample for config.properties
```
tidb.addr=127.0.0.1
tidb.port=4000
tidb.user=root
test.basepath=/Users/whoever/workspace/pingcap/tispark/integtest/testcases
```
Please add `spark.tispark.pd.addresses=127.0.0.1:2379` in `${SPARK_HOME}/conf/spark-defaults.conf` beforehand

| Key                        | Desc                                      |
| -------------------------- |:-----------------------------------------:|
| spark.tispark.pd.addresses | Placement Driver Address separated by "," |
| tidb.addr      | TiDB Address      |
| tidb.port      | TiDB Port      |
| tidb.user      | TiDB username |
| test.basepath | Test case base path include .ddl, .sql and .data files |
| test.mode     | Test: Run test only; Load: Load only; LoadNTest: Load and Test; Dump: Dump database specified by test.dumpDB.databases |
| test.dumpDB.databases  | Database to dump. Required for dump database |
| test.ignore      | Test path to ignore. Separated by comma |

