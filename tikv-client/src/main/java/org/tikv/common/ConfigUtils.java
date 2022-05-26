/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTimeZone;
import org.tikv.common.region.TiStoreType;
import org.tikv.common.types.Converter;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class ConfigUtils {

  public static final String TIKV_CONFIGURATION_FILENAME = "tikv.properties";

  public static final String TIKV_TIMEZONE = "tikv.timezone";
  public static final String TIKV_TIMEOUT = "tikv.timeout";
  public static final String TIKV_TIMEOUT_UNIT = "tikv.timeout.unit";
  public static final String TIKV_SCAN_BATCH_SIZE = "tikv.scan.batch.size";
  public static final String TIKV_IGNORE_TRUNCATE = "tikv.ignore.truncate";
  public static final String TIKV_TRUNCATE_AS_WARNING = "tikv.truncate.as.warning";
  public static final String TIKV_MAX_FRAME_SIZE = "tikv.max.frame.size";
  public static final String TIKV_INDEX_SCAN_BATCH_SIZE = "tikv.index.scan.batch.size";
  public static final String TIKV_REGION_SCAN_DOWNGRADE_THRESHOLD =
      "tikv.region.scan.downgrade.threshold";
  public static final String TIKV_MAX_REQUEST_KEY_RANGE_SIZE = "tikv.max.request.key.range.size";
  public static final String TIKV_INDEX_SCAN_CONCURRENCY = "tikv.index.scan.concurrency";
  public static final String TIKV_TABLE_SCAN_CONCURRENCY = "tikv.table.scan.concurrency";
  public static final String TIKV_BATCH_GET_CONCURRENCY = "tikv.batch.get.concurrency";
  public static final String TIKV_BATCH_PUT_CONCURRENCY = "tikv.batch.put.concurrency";
  public static final String TIKV_BATCH_DELETE_CONCURRENCY = "tikv.batch.delete.concurrency";
  public static final String TIKV_BATCH_SCAN_CONCURRENCY = "tikv.batch.scan.concurrency";

  public static final String TIKV_DELETE_RANGE_CONCURRENCY = "tikv.delete.range.concurrency";

  public static final String TIKV_COMMAND_PRIORITY = "tikv.command.priority";
  public static final String TIKV_ISOLATION_LEVEL = "tikv.isolation.level";
  public static final String TIKV_SHOW_ROWID = "tikv.show.rowid";

  public static final String TIKV_DB_PREFIX = "tikv.db.prefix";
  public static final String TIKV_WRITE_ENABLE = "tikv.write.enable";
  public static final String TIKV_WRITE_ALLOW_SPARK_SQL = "tikv.write.allow.spark.sql";

  public static final String TIKV_WRITE_WITHOUT_LOCK_TABLE = "tikv.write.without.lock.table";
  public static final String TIKV_REGION_SPLIT_SIZE_IN_MB = "tikv.region.split.size.in.mb";
  public static final String TIKV_PARTITION_PER_SPLIT = "tikv.partition.per.split";
  public static final String TIKV_KV_CLIENT_CONCURRENCY = "tikv.kv.client.concurrency";

  // ---------------------------------------------------------------------
  public static final DateTimeZone DEF_TIMEZONE = Converter.getLocalTimezone();
  public static final int DEF_TIMEOUT = 10;
  public static final TimeUnit DEF_TIMEOUT_UNIT = TimeUnit.MINUTES;
  public static final int DEF_SCAN_BATCH_SIZE = 10480;
  public static final boolean DEF_IGNORE_TRUNCATE = true;
  public static final boolean DEF_TRUNCATE_AS_WARNING = false;
  public static final int DEF_MAX_FRAME_SIZE = 2147483647; // 2 GB
  public static final int DEF_INDEX_SCAN_BATCH_SIZE = 20000;
  public static final int DEF_REGION_SCAN_DOWNGRADE_THRESHOLD = 10000000;
  // if keyRange size per request exceeds this limit, the request might be too large to be accepted
  // by TiKV(maximum request size accepted by TiKV is around 1MB)
  public static final int MAX_REQUEST_KEY_RANGE_SIZE = 20000;
  public static final int DEF_INDEX_SCAN_CONCURRENCY = 5;
  public static final int DEF_TABLE_SCAN_CONCURRENCY = 512;
  public static final int DEF_BATCH_GET_CONCURRENCY = 20;
  public static final int DEF_BATCH_PUT_CONCURRENCY = 20;
  public static final int DEF_BATCH_DELETE_CONCURRENCY = 20;
  public static final int DEF_BATCH_SCAN_CONCURRENCY = 5;
  public static final int DEF_DELETE_RANGE_CONCURRENCY = 20;
  public static final CommandPri DEF_COMMAND_PRIORITY = CommandPri.Low;
  public static final IsolationLevel DEF_ISOLATION_LEVEL = IsolationLevel.SI;
  public static final boolean DEF_SHOW_ROWID = false;
  public static final String DEF_DB_PREFIX = "";
  public static final boolean DEF_WRITE_ENABLE = true;
  public static final boolean DEF_WRITE_ALLOW_SPARK_SQL = false;
  public static final boolean DEF_WRITE_WITHOUT_LOCK_TABLE = false;
  public static final int DEF_TIKV_REGION_SPLIT_SIZE_IN_MB = 96;
  public static final int DEF_PARTITION_PER_SPLIT = 10;
  public static final int DEF_KV_CLIENT_CONCURRENCY = 10;
  public static final List<TiStoreType> DEF_ISOLATION_READ_ENGINES =
      ImmutableList.of(TiStoreType.TiKV, TiStoreType.TiFlash);
}
