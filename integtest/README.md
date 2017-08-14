## How to do Integration Test
It requires both TiDB and Spark present. It will run Spark SQL and compare result with TiDB.
User might attach to an existing database with existing data for testing or dump test data and schema via this tool to create testcases as well.
Put config.properties into Spark's conf path
Test cases will be searched recursively. DDL files is for recreate test table; SQL is for test script run for both sides. Data file is for loading test data.

Dump database to create test data:
```
./dump.sh
```

Load test data from dumping files:
```
./load.sh
```

Run Test on existing data:
```
./test.sh
```

And you might run manually like this for other use cases:
```
java -Dtest.mode=Load -cp ./conf:$./lib/* com.pingcap.spark.TestFramework
```

### Configuration
Here is a sample for config.properties
```
pdaddr=127.0.0.1:2379
tidbaddr=127.0.0.1
tidbport=4000
tidbuser=root
testbasepath=/Users/whoever/workspace/pingcap/tispark/integtest/testcases
```

| Key           | Desc          |
| ------------- |:-------------:|
| pdaddr        | Placement Driver Address |
| tidbaddr      | TiDB Address      |
| tidbport      | TiDB Port      |
| tidbuser      | TiDB username |
| test.basepath | Test case base path include .ddl, .sql and .data files | 
| test.mode     | Test: Run test only; Load: Load only; LoadNTest: Load and Test; Dump: Dump database specified by test.dumpDB.databases |
| test.dumpDB.databases  | Database to dump. Required for dump database |
| test.ignore      | Test path to ignore. |
