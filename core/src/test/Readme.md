## Integration Test
### Usage
1. Prepare your Spark and TiDB cluster environment, it is recommended to use our [docker-compose](../../../docker-compose.yaml) script to deploy a TiDB cluster locally, or you may prefer other ways to set up an appropriate test environment.
2. You can modify test configuration placed in [tidb_config.properties.template](./resources/tidb_config.properties.template), copy this template as tidb_config.properties and config this file according to your environment.
3. Run `mvn test` to run all tests.

### Configuration
You can change your test configuration in `tidb_config.properties`
```bash
# TiDB address
tidb.addr=127.0.0.1
# TiDB port
tidb.port=4000
# TiDB login user
tidb.user=root
# TPCH database name, if you already have a tpch database in TiDB, specify the db name so that TPCH tests will run on this database
tpch.db=tpch_test
# Placement Driver address:port
spark.tispark.pd.addresses=127.0.0.1:2379
# Whether to allow index read in tests, you must set this to true to run index tests.
spark.tispark.plan.allow_index_double_read=true
# Whether to load test data before running tests. If you haven't load tispark_test or tpch_test data, set this to true. The next time you run tests, you can set this to false.
test.data.load=false
```