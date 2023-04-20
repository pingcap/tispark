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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark

object TiConfigConst {
  val PD_ADDRESSES: String = "spark.tispark.pd.addresses"
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
  val USE_INDEX_SCAN_FIRST: String = "spark.tispark.plan.use_index_scan_first"
  val COPROCESS_STREAMING: String = "spark.tispark.coprocess.streaming"
  val CODEC_FORMAT: String = "spark.tispark.coprocess.codec_format"
  val UNSUPPORTED_PUSHDOWN_EXPR: String = "spark.tispark.plan.unsupported_pushdown_exprs"
  val CHUNK_BATCH_SIZE: String = "spark.tispark.coprocessor.chunk_batch_size"
  val REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD: String =
    "spark.tispark.plan.downgrade.index_threshold"
  val UNSUPPORTED_TYPES: String = "spark.tispark.type.unsupported_mysql_types"
  val ENABLE_AUTO_LOAD_STATISTICS: String = "spark.tispark.statistics.auto_load"
  val CACHE_EXPIRE_AFTER_ACCESS: String = "spark.tispark.statistics.expire_after_access"
  val SHOW_ROWID: String = "spark.tispark.show_rowid"
  val DB_PREFIX: String = "spark.tispark.db_prefix"
  val WRITE_ALLOW_SPARK_SQL: String = "spark.tispark.write.allow_spark_sql"
  val WRITE_ENABLE: String = "spark.tispark.write.enable"
  val WRITE_WITHOUT_LOCK_TABLE: String = "spark.tispark.write.without_lock_table"
  val TIKV_REGION_SPLIT_SIZE_IN_MB: String = "spark.tispark.tikv.region_split_size_in_mb"
  val ISOLATION_READ_ENGINES: String = "spark.tispark.isolation_read_engines"
  val PARTITION_PER_SPLIT: String = "spark.tispark.partition_per_split"
  val KV_CLIENT_CONCURRENCY: String = "spark.tispark.kv_client_concurrency"

  val SNAPSHOT_ISOLATION_LEVEL: String = "SI"
  val READ_COMMITTED_ISOLATION_LEVEL: String = "RC"

  val TIKV_STORAGE_ENGINE: String = "tikv"
  val TIFLASH_STORAGE_ENGINE: String = "tiflash"
  val DEFAULT_STORAGE_ENGINES: String = "tikv,tiflash"

  val CHUNK_CODEC_FORMAT: String = "chunk"
  val DEFAULT_CODEC_FORMAT: String = "chblock"

  // TLS
  val TIKV_TLS_ENABLE: String = "spark.tispark.tikv.tls_enable"
  val TIKV_TRUST_CERT_COLLECTION: String = "spark.tispark.tikv.trust_cert_collection"
  val TIKV_KEY_CERT_CHAIN: String = "spark.tispark.tikv.key_cert_chain"
  val TIKV_KEY_FILE: String = "spark.tispark.tikv.key_file"
  val TIKV_JKS_ENABLE: String = "spark.tispark.tikv.jks_enable"
  val TIKV_JKS_KEY_PATH: String = "spark.tispark.tikv.jks_key_path"
  val TIKV_JKS_KEY_PASSWORD: String = "spark.tispark.tikv.jks_key_password"
  val TIKV_JKS_TRUST_PATH: String = "spark.tispark.tikv.jks_trust_path"
  val TIKV_JKS_TRUST_PASSWORD: String = "spark.tispark.tikv.jks_trust_password"
  val TIKV_TLS_RELOAD_INTERVAL: String = "spark.tispark.tikv.tls_reload_interval"
  val TIKV_CONN_RECYCLE_TIME: String = "spark.tispark.tikv.conn_recycle_time"

  val STALE_READ: String = "spark.tispark.stale_read"
  val DEFAULT_STALE_READ: String = ""

  val TELEMETRY_ENABEL: String = "spark.tispark.telemetry.enable"

  val HOST_MAPPING: String = "spark.tispark.host_mapping"

  val NEW_COLLATION_ENABLE: String = "spark.tispark.new_collation_enable"

  // GC
  val GC_MAX_WAIT_TIME: String = "spark.tispark.gc_max_wait_time"
  val DEFAULT_GC_MAX_WAIT_TIME: Long = 24 * 60 * 60
  val DEFAULT_GC_SAFE_POINT_TTL: Int = 5 * 60

  // cache load
  val LOAD_TABLES: String = "spark.tispark.load_tables"
  val DEFAULT_LOAD_TABLES: Boolean = true
}
