## Integration Test

### Usage

1. Prepare your TiDB cluster environment. See [here](../../../docs/dev/build/start_tidb_cluster.md) for more details.

2. Run `mvn test -am -pl core -Dtest=moo -DwildcardSuites=com.pingcap.tispark.xxx.xxxSuite` to run a test.

3. It is not recommended running all the test. You can run all the tests when you submit a pr with `/run-all-tests` comment.

### Configuration

You can change your test configuration in `core/src/test/resources/tidb_config.properties` according to `core/src/test/resources/tidb_config.properties.template`.

## TLS Test

TLS test is used to test TiSpark TSL function.
All certs used in test can be found in `config/cert`.

1. Set up TiDB cluster with TLS enable in docker container

```shell
docker-compose -f docker-compose-TiDB-TLS.yaml up
```

2. Copy cert to root directory

```shell
sudo cp -r ./config /
```

3. Add hosts. Because pd is running in docker by docker-compose, and its name in the docker-compose file is `pd`, we should configure the correct host for TiDB to find it.

```shell
sduo echo -e "127.0.0.1   pd \n127.0.0.1   tikv" | sudo tee -a /etc/hosts
```

4. Set TLS configurations. Just use template or you can modify by yourself.

```shell
cp core/src/test/resources/tidb_config.properties.TLS.template core/src/test/resources/tidb_config.properties
```

5. Run tests

```shell
run: mvn test -am -pl core -Dtest=moo -DwildcardSuites=com.pingcap.tispark.tls.JDBCTLSSuite,com.pingcap.tispark.tls.TiKVClientTLSSuite,com.pingcap.tispark.tls.TiSparkTLSSuite -DfailIfNoTests=false
```