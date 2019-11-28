/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark

object TiConfigConst {
  val PD_ADDRESSES: String = "spark.tispark.pd.addresses"
  val USE_CATALOG_PLUGIN: String = "spark.tispark.catalog.plugin_spark_3_0"
  val GRPC_FRAME_SIZE: String = "spark.tispark.grpc.framesize"
  val GRPC_TIMEOUT: String = "spark.tispark.grpc.timeout_in_sec"
  val GRPC_RETRY_TIMES: String = "spark.tispark.grpc.retry.times"
  val INDEX_SCAN_BATCH_SIZE: String = "spark.tispark.index.scan_batch_size"
  val INDEX_SCAN_CONCURRENCY: String = "spark.tispark.index.scan_concurrency"
  val TABLE_SCAN_CONCURRENCY: String = "spark.tispark.table.scan_concurrency"
  val ALLOW_AGG_PUSHDOWN: String = "spark.tispark.plan.allow_agg_pushdown"
  val REQUEST_COMMAND_PRIORITY: String = "spark.tispark.request.command.priority"
  val REQUEST_ISOLATION_LEVEL: String = "spark.tispark.request.isolation.level"
  val ALLOW_INDEX_READ: String = "spark.tispark.plan.allow_index_read"
  val COPROCESS_STREAMING: String = "spark.tispark.coprocess.streaming"
  val UNSUPPORTED_PUSHDOWN_EXPR: String = "spark.tispark.plan.unsupported_pushdown_exprs"
  val REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD: String = "spark.tispark.plan.downgrade.index_threshold"
  val UNSUPPORTED_TYPES: String = "spark.tispark.type.unsupported_mysql_types"
  val ENABLE_AUTO_LOAD_STATISTICS: String = "spark.tispark.statistics.auto_load"
  val CACHE_EXPIRE_AFTER_ACCESS: String = "spark.tispark.statistics.expire_after_access"
  val SHOW_ROWID: String = "spark.tispark.show_rowid"
  val DB_PREFIX: String = "spark.tispark.db_prefix"
  val WRITE_ALLOW_SPARK_SQL: String = "spark.tispark.write.allow_spark_sql"
  val WRITE_ENABLE: String = "spark.tispark.write.enable"
  val WRITE_WITHOUT_LOCK_TABLE: String = "spark.tispark.write.without_lock_table"
  val TIKV_REGION_SPLIT_SIZE_IN_MB: String = "spark.tispark.tikv.region_split_size_in_mb"
  val USE_TIFLASH: String = "spark.tispark.use.tiflash"
  val ENABLE_TIFLASH_TEST: String = "spark.tispark.enable.tiflash_test"

  val SNAPSHOT_ISOLATION_LEVEL: String = "SI"
  val READ_COMMITTED_ISOLATION_LEVEL: String = "RC"
}
