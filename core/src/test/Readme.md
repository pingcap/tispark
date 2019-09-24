## Integration Test
### Usage
1. Prepare your Spark and TiDB cluster environment, it is recommended to use our [docker-compose](../../../docker-compose.yaml) script to deploy a TiDB cluster locally, or you may prefer other ways to set up an appropriate test environment.
2. You can modify test configuration placed in [tidb_config.properties.template](./resources/tidb_config.properties.template), copy this template as tidb_config.properties and config this file according to your environment.
3. Run `mvn test` to run all tests.

### Configuration
You can change your test configuration in `core/src/test/resources/tidb_config.properties` according to `core/src/test/resources/tidb_config.properties.template`.