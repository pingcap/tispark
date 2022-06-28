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

package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.pd.PDUtils;
import com.pingcap.tikv.region.TiStoreType;
import com.pingcap.tikv.types.Converter;
import com.pingcap.tikv.util.HostMapping;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.joda.time.DateTimeZone;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

@Getter
@Setter
@Accessors(chain = true)
public class TiConfiguration implements Serializable {
  private static final DateTimeZone DEF_TIMEZONE = Converter.getLocalTimezone();
  private static final int DEF_TIMEOUT = 10;
  private static final TimeUnit DEF_TIMEOUT_UNIT = TimeUnit.MINUTES;
  private static final int DEF_SCAN_BATCH_SIZE = 10480;
  private static final boolean DEF_IGNORE_TRUNCATE = true;
  private static final boolean DEF_TRUNCATE_AS_WARNING = false;
  private static final int DEF_MAX_FRAME_SIZE = 2147483647; // 2 GB
  private static final int DEF_INDEX_SCAN_BATCH_SIZE = 20000;
  private static final int DEF_REGION_SCAN_DOWNGRADE_THRESHOLD = 10000000;
  // if keyRange size per request exceeds this limit, the request might be too large to be accepted
  // by TiKV(maximum request size accepted by TiKV is around 1MB)
  private static final int MAX_REQUEST_KEY_RANGE_SIZE = 20000;
  private static final int DEF_INDEX_SCAN_CONCURRENCY = 5;
  private static final int DEF_TABLE_SCAN_CONCURRENCY = 512;
  private static final int DEF_BATCH_GET_CONCURRENCY = 20;
  private static final int DEF_BATCH_PUT_CONCURRENCY = 20;
  private static final int DEF_BATCH_DELETE_CONCURRENCY = 20;
  private static final int DEF_BATCH_SCAN_CONCURRENCY = 5;
  private static final int DEF_DELETE_RANGE_CONCURRENCY = 20;
  private static final CommandPri DEF_COMMAND_PRIORITY = CommandPri.Low;
  private static final IsolationLevel DEF_ISOLATION_LEVEL = IsolationLevel.SI;
  private static final boolean DEF_SHOW_ROWID = false;
  private static final String DEF_DB_PREFIX = "";
  private static final boolean DEF_WRITE_ENABLE = true;
  private static final boolean DEF_WRITE_ALLOW_SPARK_SQL = false;
  private static final boolean DEF_WRITE_WITHOUT_LOCK_TABLE = false;
  private static final int DEF_TIKV_REGION_SPLIT_SIZE_IN_MB = 96;
  private static final int DEF_PARTITION_PER_SPLIT = 10;
  private static final int DEF_KV_CLIENT_CONCURRENCY = 10;
  private static final List<TiStoreType> DEF_ISOLATION_READ_ENGINES =
      ImmutableList.of(TiStoreType.TiKV, TiStoreType.TiFlash);
  private static final String DEF_TIKV_CONN_RECYCLE_TIME = "60s";
  private static final String DEF_TIKV_TLS_RELOAD_INTERVAL = "10s";

  private int timeout = DEF_TIMEOUT;
  private TimeUnit timeoutUnit = DEF_TIMEOUT_UNIT;
  private boolean ignoreTruncate = DEF_IGNORE_TRUNCATE;
  private boolean truncateAsWarning = DEF_TRUNCATE_AS_WARNING;
  private int maxFrameSize = DEF_MAX_FRAME_SIZE;
  private List<URI> pdAddrs = new ArrayList<>();
  private int indexScanBatchSize = DEF_INDEX_SCAN_BATCH_SIZE;
  private int downgradeThreshold = DEF_REGION_SCAN_DOWNGRADE_THRESHOLD;
  private int indexScanConcurrency = DEF_INDEX_SCAN_CONCURRENCY;
  private int tableScanConcurrency = DEF_TABLE_SCAN_CONCURRENCY;
  private int batchGetConcurrency = DEF_BATCH_GET_CONCURRENCY;
  private int batchPutConcurrency = DEF_BATCH_PUT_CONCURRENCY;
  private int batchDeleteConcurrency = DEF_BATCH_DELETE_CONCURRENCY;
  private int batchScanConcurrency = DEF_BATCH_SCAN_CONCURRENCY;
  private int deleteRangeConcurrency = DEF_DELETE_RANGE_CONCURRENCY;
  private CommandPri commandPriority = DEF_COMMAND_PRIORITY;
  private IsolationLevel isolationLevel = DEF_ISOLATION_LEVEL;
  private int maxRequestKeyRangeSize = MAX_REQUEST_KEY_RANGE_SIZE;
  private boolean showRowId = DEF_SHOW_ROWID;
  private String DBPrefix = DEF_DB_PREFIX;

  private boolean writeAllowSparkSQL = DEF_WRITE_ALLOW_SPARK_SQL;
  private boolean writeEnable = DEF_WRITE_ENABLE;
  private boolean writeWithoutLockTable = DEF_WRITE_WITHOUT_LOCK_TABLE;
  private int tikvRegionSplitSizeInMB = DEF_TIKV_REGION_SPLIT_SIZE_IN_MB;
  private int partitionPerSplit = DEF_PARTITION_PER_SPLIT;

  private int kvClientConcurrency = DEF_KV_CLIENT_CONCURRENCY;

  private List<TiStoreType> isolationReadEngines = DEF_ISOLATION_READ_ENGINES;

  // TLS configure
  private boolean tlsEnable = false;
  private String trustCertCollectionFile;
  private String keyCertChainFile;
  private String keyFile;
  private boolean jksEnable = false;
  private String jksKeyPath;
  private String jksKeyPassword;
  private String jksTrustPath;
  private String jksTrustPassword;
  private long connRecycleTime = getTimeAsSeconds(DEF_TIKV_CONN_RECYCLE_TIME);
  private long certReloadInterval = getTimeAsSeconds(DEF_TIKV_TLS_RELOAD_INTERVAL);

  private String networkMappingName = "";
  private HostMapping hostMapping = null;

  private static Long getTimeAsSeconds(String key) {
    return Utils.timeStringAsSec(key);
  }

  public static TiConfiguration createDefault(String pdAddrsStr) {
    Objects.requireNonNull(pdAddrsStr, "pdAddrsStr is null");
    TiConfiguration conf = new TiConfiguration();
    conf.pdAddrs = strToURI(pdAddrsStr);
    return conf;
  }

  private static List<URI> strToURI(String addressStr) {
    Objects.requireNonNull(addressStr);
    String[] addrs = addressStr.split(",");
    Arrays.sort(addrs);
    return PDUtils.addrsToUrls(addrs);
  }

  public static <E> String listToString(List<E> list) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < list.size(); i++) {
      sb.append(list.get(i).toString());
      if (i != list.size() - 1) {
        sb.append(",");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  public DateTimeZone getLocalTimeZone() {
    return DEF_TIMEZONE;
  }

  public String getPdAddrsString() {
    return listToString(pdAddrs);
  }

  public int getScanBatchSize() {
    return DEF_SCAN_BATCH_SIZE;
  }

  public void setMaxRequestKeyRangeSize(int maxRequestKeyRangeSize) {
    if (maxRequestKeyRangeSize <= 0) {
      throw new IllegalArgumentException("Key range size cannot be less than 1");
    }
    this.maxRequestKeyRangeSize = maxRequestKeyRangeSize;
  }

  public TiConfiguration setConnRecycleTimeInSeconds(String connRecycleTime) {
    this.connRecycleTime = getTimeAsSeconds(connRecycleTime);
    return this;
  }

  public TiConfiguration setCertReloadIntervalInSeconds(String interval) {
    this.certReloadInterval = getTimeAsSeconds(interval);
    return this;
  }
}
