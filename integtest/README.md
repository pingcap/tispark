## How to do Integration Test
It requires both TiDB and Spark present. It will run Spark SQL and compare result with TiDB.
User might attach to an existing database with existing data for testing or dump test data and schema via this tool to create testcases as well.
Put config.properties into Spark's conf path
Test cases will be searched recursively. DDL files is for recreate test table; SQL is for test script run for both sides. Data file is for loading test data.

### Configuration
Here is a Sample for config.properties
```
pdaddr=127.0.0.1:2379
tidbaddr=127.0.0.1
tidbport=4000
tidbuser=root
testbasepath=/Users/whoever/workspace/pingcap/tispark/integtest/testcases
dumpDB=false
database=tpch
loaddata=false
```

| Key           | Desc          |
| ------------- |:-------------:|
| pdaddr        | Placement Driver Address |
| tidbaddr      | TiDB Address      |
| tidbport      | TiDB Port      |
| tidbuser      | TiDB username |
| testbasepath  | Test case base path include .ddl, .sql and .data files | 
| dumpDB      | If do database dumping to create test data files or run test. Put true will not run test. |
| database      | Database to attach. Required for dump database; Optional for load and test; If absent for a temp sandbox database will be created |
| loaddata      | Create table and load data before test. |

