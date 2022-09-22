# Telemetry
By default, TiSpark collect usage information and share the information with PingCAP. You can close it by configuring
`spark.tispark.telemetry.enable = false` in `spark-default.conf`.

When the telemetry collection feature is enabled, usage information will be shared, including(but not limited to):
* A randomly generated telemetry ID.
* OS and hardware information, such as OS name, OS version, CPU, memory, and disk.
* A part of TiSpark configuration.

To view the full content of telemetry, set the level `log4j.logger.com.pingcap.tispark` lower than `INFO` in
`${SPARK_HOME}/conf/log4j.properties`. And the telemetry content will be saved in the log file.
```shell
22/05/05 14:22:21 INFO Telemetry: Telemetry report: {"track_id":"trkid_7178a3d8-da26-41ca-a7a3-00e165877bda","time":"2022-05-04 23:22:20","hardware":{"os":"Ubuntu","disks":[{"name":"/dev/nvme0n1","size":"512110190592"}],"version":"20.04.4 LTS (Focal Fossa) build 5.13.0-41-generic","cpu":{"model":"11th Gen Intel(R) Core(TM) i7-1160G7 @ 1.20GHz","logicalCores":"8","physicalCores":"4"},"memory":"Available: 828.1 MiB/15.4 GiB"},"instance":{"tispark_version":"2.5.0-SNAPSHOT","tidb_version":"v6.0.0","spark_version":"3.0.2"},"configuration":{"spark.tispark.plan.use_index_scan_first":"false","spark.tispark.index.scan_concurrency":"5","spark.tispark.type.unsupported_mysql_types":"","spark.tispark.plan.allow_index_read":"true","spark.tispark.request.command.priority":"LOW","spark.tispark.request.isolation.level":"SI","spark.tispark.show_rowid":"false","spark.tispark.plan.allow_agg_pushdown":"false","spark.tispark.isolation_read_engines":"tikv","spark.sql.auth.enable":"true","spark.tispark.coprocessor.chunk_batch_size":"1024","spark.tispark.index.scan_batch_size":"20000","spark.tispark.coprocess.codec_format":"chblock","spark.tispark.coprocess.streaming":"false"}}
```

An entry table of telemetry is shown here.

| Field name                                 | Description                        |
|--------------------------------------------|------------------------------------|
| track_id                                   | A randomly generated telemetry ID  |
| time                                       | The time point of reporting        |
| hardware.os                                | Operating system name              |
| hardware.version                           | Operating system version           |
| hardware.cpu.model                         | CPU model                          |
| hardware.cpu.logicalCores                  | Number of CPU logical cores        |
| hardware.cpu.physicalCores                 | Number of CPU physical cores       |
| hardware.memory                            | Memory capacity                    |
| hardware.disks.name                        | Disks name                         |
| hardware.disks.size                        | Disks capacity                     |
| instance.tispark_version                   | TiSpark version                    |
| instance.tidb_version                      | TiDB version                       |
| instance.spark_version                     | Spark version                      |
| spark.tispark.plan.use_index_scan_first    | TiSpark Configuration              |
| spark.tispark.index.scan_concurrency       | TiSpark Configuration              |
| spark.tispark.type.unsupported_mysql_types | TiSpark Configuration              |
| spark.tispark.plan.allow_index_read        | TiSpark Configuration              |
| spark.tispark.request.command.priority     | TiSpark Configuration              |
| spark.tispark.request.isolation.level      | TiSpark Configuration              |
| spark.tispark.show_rowid                   | TiSpark Configuration              |
| spark.tispark.plan.allow_agg_pushdown      | TiSpark Configuration              |
| spark.tispark.isolation_read_engines       | TiSpark Configuration              |
| spark.tispark.coprocessor.chunk_batch_size | TiSpark Configuration              |
| spark.tispark.index.scan_batch_size        | TiSpark Configuration              |
| spark.tispark.coprocess.codec_format       | TiSpark Configuration              |
| spark.tispark.coprocess.streaming          | TiSpark Configuration              |
| spark.sql.auth.enable                      | TiSpark Configuration              |
