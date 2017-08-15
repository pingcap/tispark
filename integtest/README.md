## How to do Integration Test
It requires both TiDB and Spark present. It will run Spark SQL and compare result with TiDB.
User might attach to an existing database with existing data for testing or dump test data and schema via this tool to create testcases as well.
Put config.properties into Spark's conf path
Test cases will be searched recursively. DDL files is for recreate test table; SQL is for test script run for both sides. Data file is for loading test data.

Dump databases listed to create test data:
```
./dump.sh dbnameList
```
Database name list is separated by comma like this: db1, db2

Load test data from dumping files:
```
./load.sh 
```

Run Test on existing data:
```
./test.sh [-d|--debug]
```
If debug flag present, JVM remote debug port will open at 5005.

And you might run manually like this for other use cases:
```
java -Dtest.mode=Load -cp ./conf:$./lib/* com.pingcap.spark.TestFramework
```

### Configuration
Here is a sample for config.properties
```
pd.addrs=127.0.0.1:2379
tidb.addr=127.0.0.1
tidb.port=4000
tidb.user=root
test.basepath=/Users/whoever/workspace/pingcap/tispark/integtest/testcases
```

| Key           | Desc          |
| ------------- |:-------------:|
| pd.addrs        | Placement Driver Address separated by "," |
| tidb.addr      | TiDB Address      |
| tidb.port      | TiDB Port      |
| tidb.user      | TiDB username |
| test.basepath | Test case base path include .ddl, .sql and .data files | 
| test.mode     | Test: Run test only; Load: Load only; LoadNTest: Load and Test; Dump: Dump database specified by test.dumpDB.databases |
| test.dumpDB.databases  | Database to dump. Required for dump database |
| test.ignore      | Test path to ignore. Separated by comma |
