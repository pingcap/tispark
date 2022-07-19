# TiSpark Configurations

> The configurations in the table below can be put together with `spark-defaults.conf` or passed in the same way as other Spark configuration properties.


| Key                                             | Default Value    | Description                                                                                                                                                                                                                                                                                                                        |
|-------------------------------------------------|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `spark.tispark.pd.addresses`                    | `127.0.0.1:2379` | The addresses of PD cluster, which are split by comma                                                                                                                                                                                                                                                                              |
| `spark.tispark.grpc.framesize`                  | `2147483647`     | The maximum frame size of gRPC response in bytes (default 2G)                                                                                                                                                                                                                                                                      |
| `spark.tispark.grpc.timeout_in_sec`             | `10`             | The gRPC timeout time in seconds                                                                                                                                                                                                                                                                                                   |
| `spark.tispark.plan.allow_agg_pushdown`         | `true`           | Whether aggregations are allowed to push down to TiKV (in case of busy TiKV nodes)                                                                                                                                                                                                                                                 |
| `spark.tispark.plan.allow_index_read`           | `true`           | Whether index is enabled in planning (which might cause heavy pressure on TiKV)                                                                                                                                                                                                                                                    |
| `spark.tispark.index.scan_batch_size`           | `20000`          | The number of row key in batch for the concurrent index scan                                                                                                                                                                                                                                                                       |
| `spark.tispark.index.scan_concurrency`          | `5`              | The maximal number of threads for index scan that retrieves row keys (shared among tasks inside each JVM)                                                                                                                                                                                                                          |
| `spark.tispark.table.scan_concurrency`          | `512`            | The maximal number of threads for table scan (shared among tasks inside each JVM)                                                                                                                                                                                                                                                  |
| `spark.tispark.request.command.priority`        | `Low`            | The value options are `Low`, `Normal`, `High`. This setting impacts the resource to get in TiKV. `Low` is recommended because the OLTP workload is not disturbed.                                                                                                                                                                  |
| `spark.tispark.coprocess.codec_format`          | `chblock`        | choose the default codec format for coprocessor, available options are `default`, `chblock`, `chunk`                                                                                                                                                                                                                               |
| `spark.tispark.coprocess.streaming`             | `false`          | Whether to use streaming for response fetching (experimental)                                                                                                                                                                                                                                                                      |
| `spark.tispark.plan.unsupported_pushdown_exprs` | ``               | A comma-separated list of expressions. In case you have a very old version of TiKV, you might disable some of the expression push-down if they are not supported.                                                                                                                                                                  |
| `spark.tispark.plan.downgrade.index_threshold`  | `1000000000`     | If the range of index scan on one Region exceeds this limit in the original request, downgrade this Region's request to table scan rather than the planned index scan. By default, the downgrade is disabled.                                                                                                                      |
| `spark.tispark.show_rowid`                      | `false`          | Whether to show the implicit row ID if the ID exists                                                                                                                                                                                                                                                                               |
| `spark.tispark.db_prefix`                       | ``               | The string that indicates the extra prefix for all databases in TiDB. This string distinguishes the databases in TiDB from the Hive databases with the same name.                                                                                                                                                                  |
| `spark.tispark.request.isolation.level`         | `SI`             | Isolation level means whether to resolve locks for the underlying TiDB clusters. When you use the "RC", you get the latest version of record smaller than your `tso` and ignore the locks. If you use "SI", you resolve the locks and get the records depending on whether the resolved lock is committed or aborted.              |
| `spark.tispark.coprocessor.chunk_batch_size`    | `1024`           | How many rows fetched from Coprocessor                                                                                                                                                                                                                                                                                             |
| `spark.tispark.isolation_read_engines`          | `tikv,tiflash`   | List of readable engines of TiSpark, comma separated, storage engines not listed will not be read                                                                                                                                                                                                                                  |
| `spark.tispark.stale_read`                      | it is optional   | The stale read timestamp(ms). see [here](stale_read.md) for more detail                                                                                                                                                                                                                                                            |
| `spark.tispark.tikv.tls_enable`                 | `false`          | Whether to enable TiSpark TLS. 　                                                                                                                                                                                                                                                                                                   |
| `spark.tispark.tikv.trust_cert_collection`      | ``               | Trusted certificates for TiKV Client, which is used for verifying the remote pd's certificate, e.g. `/home/tispark/config/root.pem` The file should contain an X.509 certificate collection.                                                                                                                                       |
| `spark.tispark.tikv.key_cert_chain`             | ``               | An X.509 certificate chain file for TiKV Client, e.g. `/home/tispark/config/client.pem`.                                                                                                                                                                                                                                           |
| `spark.tispark.tikv.key_file`                   | ``               | A PKCS#8 private key file for TiKV Client, e.g. `/home/tispark/client_pkcs8.key`.                                                                                                                                                                                                                                                  |
| `spark.tispark.tikv.jks_enable`                 | `false`          | Whether to use the JAVA key store instead of the X.509 certificate.                                                                                                                                                                                                                                                                |
| `spark.tispark.tikv.jks_trust_path`             | ``               | A JKS format certificate for TiKV Client, that is generated by `keytool`, e.g. `/home/tispark/config/tikv-truststore`.                                                                                                                                                                                                             |
| `spark.tispark.tikv.jks_trust_password`         | ``               | The password of `spark.tispark.tikv.jks_trust_path`.                                                                                                                                                                                                                                                                               |
| `spark.tispark.tikv.jks_key_path`               | ``               | A JKS format key for TiKV Client generated by `keytool`, e.g. `/home/tispark/config/tikv-clientstore`.                                                                                                                                                                                                                             |
| `spark.tispark.tikv.jks_key_password`           | ``               | The password of `spark.tispark.tikv.jks_key_path`.                                                                                                                                                                                                                                                                                 |
| `spark.tispark.jdbc.tls_enable`                 | `false`          | Whether to enable TLS when using JDBC connector.                                                                                                                                                                                                                                                                                   |
| `spark.tispark.jdbc.server_cert_store`          | ``               | Trusted certificates for JDBC. This is a JKS format certificate generated by `keytool`, e.g. `/home/tispark/config/jdbc-truststore`. Default is "", which means TiSpark doesn't verify TiDB server.                                                                                                                                |
| `spark.tispark.jdbc.server_cert_password`       | ``               | The password of `spark.tispark.jdbc.server_cert_store`.                                                                                                                                                                                                                                                                            |
| `spark.tispark.jdbc.client_cert_store`          | ``               | A PKCS#12 certificate for JDBC. It is a JKS format certificate generated by `keytool`, e.g. `/home/tispark/config/jdbc-clientstore`. Default is "", which means TiDB server doesn't verify TiSpark.                                                                                                                                |
| `spark.tispark.jdbc.client_cert_password`       | ``               | The password of `spark.tispark.jdbc.client_cert_store`.                                                                                                                                                                                                                                                                            |
| `spark.tispark.tikv.tls_reload_interval`        | `10s`            | The interval time of checking if there is any reloading certificates. The default is `10s` (10 seconds).                                                                                                                                                                                                                           |
| `spark.tispark.tikv.conn_recycle_time`          | `60s`            | The interval time of cleaning the expired connection with TiKV. Only take effect when enabling cert reloading. The default is `60s` (60 seconds).                                                                                                                                                                                  |
| `spark.tispark.host_mapping`                    | ``               | This is route map used to configure public IP and intranet IP mapping. When the TiDB cluster is running on the intranet, you can map a set of intranet IPs to public IPs for an outside Spark cluster to access. The format is `{Intranet IP1}:{Public IP1};{Intranet IP2}:{Public IP2}`, e.g. `192.168.0.2:8.8.8.8;192.168.0.3:9.9.9.9`. |

### TLS Notes
TiSpark TLS has two parts: TiKV Client TLS and JDBC connector TLS. When you want to enable TLS in TiSpark, you need to configure two parts of configuration. 
`spark.tispark.tikv.xxx` is used for TiKV Client to create TLS connection with PD and TiKV server. 
While `spark.tispark.jdbc.xxx` is used for JDBC connect with TiDB server in TLS connection.

When TiSpark TLS is enabled, either the X.509 certificate with `tikv.trust_cert_collection`, `tikv.key_cert_chain` and `tikv.key_file` configurations or the JKS certificate with `tikv.jks_enable`, `tikv.jks_trust_path` and `tikv.jks_key_path` must be configured.
While `jdbc.server_cert_store` and `jdbc.client_cert_store` is optional.

TiSpark only supports TLSv1.2 and TLSv1.3 version.

* An example of opening TLS configuration with the X.509 certificate in TiKV Client.

```
spark.tispark.tikv.tls_enable                                  true
spark.tispark.tikv.trust_cert_collection                       /home/tispark/root.pem
spark.tispark.tikv.key_cert_chain                              /home/tispark/client.pem
spark.tispark.tikv.key_file                                    /home/tispark/client.key
```

* An example for enabling TLS with JKS configurations in TiKV Client.

```
spark.tispark.tikv.tls_enable                                  true
spark.tispark.tikv.jks_enable                                  true
spark.tispark.tikv.jks_key_path                                /home/tispark/config/tikv-truststore
spark.tispark.tikv.jks_key_password                            tikv_trustore_password
spark.tispark.tikv.jks_trust_path                              /home/tispark/config/tikv-clientstore
spark.tispark.tikv.jks_trust_password                          tikv_clientstore_password
```

When JKS and X.509 cert are set simultaneously, JKS would have a higher priority. 
That means TLS builder will use JKS cert first. So, do not set `spark.tispark.tikv.jks_enable=true` when you just want to use a common PEM cert.

* An example for enabling TLS in JDBC connector. 

```
spark.tispark.jdbc.tls_enable                                  true
spark.tispark.jdbc.server_cert_store                           /home/tispark/jdbc-truststore
spark.tispark.jdbc.server_cert_password                        jdbc_truststore_password
spark.tispark.jdbc.client_cert_store                           /home/tispark/jdbc-clientstore
spark.tispark.jdbc.client_cert_password                        jdbc_clientstore_password
```

For how to open TiDB TLS, see [here](https://docs.pingcap.com/tidb/dev/enable-tls-between-clients-and-servers).
For how to generate a JAVA key store, see [here](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-using-ssl.html).

### Log4j Configuration

When you start `spark-shell` or `spark-sql` and run query, you might see the following warnings:
```
Failed to get database ****, returning NoSuchObjectException
Failed to get database ****, returning NoSuchObjectException
```
where `****` is the name of database.

The warnings are benign and occurs because Spark cannot find `****` in its own catalog. You can just ignore these warnings.

To mute them, append the following text to `${SPARK_HOME}/conf/log4j.properties`.

```
# tispark disable "WARN ObjectStore:568 - Failed to get database"
log4j.logger.org.apache.hadoop.hive.metastore.ObjectStore=ERROR
```

### Time Zone Configuration

Set time zone by using the `-Duser.timezone` system property (for example, `-Duser.timezone=GMT-7`), which affects the `Timestamp` type.

Do not use `spark.sql.session.timeZone`.