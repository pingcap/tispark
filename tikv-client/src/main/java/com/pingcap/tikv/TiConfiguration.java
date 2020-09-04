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

package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.pd.PDUtils;
import com.pingcap.tikv.region.TiStoreType;
import com.pingcap.tikv.types.Converter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTimeZone;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class TiConfiguration implements Serializable {
  private static final DateTimeZone DEF_TIMEZONE = Converter.getLocalTimezone();
  private static final int DEF_TIMEOUT = 10;
  private static final TimeUnit DEF_TIMEOUT_UNIT = TimeUnit.MINUTES;
  private static final int DEF_SCAN_BATCH_SIZE = 100;
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
  private static final CommandPri DEF_COMMAND_PRIORITY = CommandPri.Low;
  private static final IsolationLevel DEF_ISOLATION_LEVEL = IsolationLevel.SI;
  private static final boolean DEF_SHOW_ROWID = false;
  private static final String DEF_DB_PREFIX = "";
  private static final boolean DEF_WRITE_ENABLE = true;
  private static final boolean DEF_WRITE_ALLOW_SPARK_SQL = false;
  private static final boolean DEF_WRITE_WITHOUT_LOCK_TABLE = false;
  private static final int DEF_TIKV_REGION_SPLIT_SIZE_IN_MB = 96;
  private static final int DEF_PARTITION_PER_SPLIT = 1;
  private static final int DEF_KV_CLIENT_CONCURRENCY = 10;
  private static final List<TiStoreType> DEF_ISOLATION_READ_ENGINES =
      ImmutableList.of(TiStoreType.TiKV, TiStoreType.TiFlash);

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
  private CommandPri commandPriority = DEF_COMMAND_PRIORITY;
  private IsolationLevel isolationLevel = DEF_ISOLATION_LEVEL;
  private int maxRequestKeyRangeSize = MAX_REQUEST_KEY_RANGE_SIZE;
  private boolean showRowId = DEF_SHOW_ROWID;
  private String dbPrefix = DEF_DB_PREFIX;

  private boolean writeAllowSparkSQL = DEF_WRITE_ALLOW_SPARK_SQL;
  private boolean writeEnable = DEF_WRITE_ENABLE;
  private boolean writeWithoutLockTable = DEF_WRITE_WITHOUT_LOCK_TABLE;
  private int tikvRegionSplitSizeInMB = DEF_TIKV_REGION_SPLIT_SIZE_IN_MB;
  private int partitionPerSplit = DEF_PARTITION_PER_SPLIT;

  private int kvClientConcurrency = DEF_KV_CLIENT_CONCURRENCY;

  private List<TiStoreType> isolationReadEngines = DEF_ISOLATION_READ_ENGINES;

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

  public int getTimeout() {
    return timeout;
  }

  public TiConfiguration setTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public TimeUnit getTimeoutUnit() {
    return timeoutUnit;
  }

  public TiConfiguration setTimeoutUnit(TimeUnit timeoutUnit) {
    this.timeoutUnit = timeoutUnit;
    return this;
  }

  public List<URI> getPdAddrs() {
    return pdAddrs;
  }

  public String getPdAddrsString() {
    return listToString(pdAddrs);
  }

  public int getScanBatchSize() {
    return DEF_SCAN_BATCH_SIZE;
  }

  boolean isIgnoreTruncate() {
    return ignoreTruncate;
  }

  public TiConfiguration setIgnoreTruncate(boolean ignoreTruncate) {
    this.ignoreTruncate = ignoreTruncate;
    return this;
  }

  boolean isTruncateAsWarning() {
    return truncateAsWarning;
  }

  public TiConfiguration setTruncateAsWarning(boolean truncateAsWarning) {
    this.truncateAsWarning = truncateAsWarning;
    return this;
  }

  public int getMaxFrameSize() {
    return maxFrameSize;
  }

  public TiConfiguration setMaxFrameSize(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
    return this;
  }

  public int getIndexScanBatchSize() {
    return indexScanBatchSize;
  }

  public void setIndexScanBatchSize(int indexScanBatchSize) {
    this.indexScanBatchSize = indexScanBatchSize;
  }

  public int getIndexScanConcurrency() {
    return indexScanConcurrency;
  }

  public void setIndexScanConcurrency(int indexScanConcurrency) {
    this.indexScanConcurrency = indexScanConcurrency;
  }

  public int getTableScanConcurrency() {
    return tableScanConcurrency;
  }

  public void setTableScanConcurrency(int tableScanConcurrency) {
    this.tableScanConcurrency = tableScanConcurrency;
  }

  public CommandPri getCommandPriority() {
    return commandPriority;
  }

  public void setCommandPriority(CommandPri commandPriority) {
    this.commandPriority = commandPriority;
  }

  public IsolationLevel getIsolationLevel() {
    return isolationLevel;
  }

  public void setIsolationLevel(IsolationLevel isolationLevel) {
    this.isolationLevel = isolationLevel;
  }

  public int getMaxRequestKeyRangeSize() {
    return maxRequestKeyRangeSize;
  }

  public void setMaxRequestKeyRangeSize(int maxRequestKeyRangeSize) {
    if (maxRequestKeyRangeSize <= 0) {
      throw new IllegalArgumentException("Key range size cannot be less than 1");
    }
    this.maxRequestKeyRangeSize = maxRequestKeyRangeSize;
  }

  public void setShowRowId(boolean flag) {
    this.showRowId = flag;
  }

  public boolean ifShowRowId() {
    return showRowId;
  }

  public String getDBPrefix() {
    return dbPrefix;
  }

  public void setDBPrefix(String dbPrefix) {
    this.dbPrefix = dbPrefix;
  }

  public boolean isWriteEnable() {
    return writeEnable;
  }

  public void setWriteEnable(boolean writeEnable) {
    this.writeEnable = writeEnable;
  }

  public boolean isWriteWithoutLockTable() {
    return writeWithoutLockTable;
  }

  public void setWriteWithoutLockTable(boolean writeWithoutLockTable) {
    this.writeWithoutLockTable = writeWithoutLockTable;
  }

  public boolean isWriteAllowSparkSQL() {
    return writeAllowSparkSQL;
  }

  public void setWriteAllowSparkSQL(boolean writeAllowSparkSQL) {
    this.writeAllowSparkSQL = writeAllowSparkSQL;
  }

  public int getTikvRegionSplitSizeInMB() {
    return tikvRegionSplitSizeInMB;
  }

  public void setTikvRegionSplitSizeInMB(int tikvRegionSplitSizeInMB) {
    this.tikvRegionSplitSizeInMB = tikvRegionSplitSizeInMB;
  }

  public int getDowngradeThreshold() {
    return downgradeThreshold;
  }

  public void setDowngradeThreshold(int downgradeThreshold) {
    this.downgradeThreshold = downgradeThreshold;
  }

  public int getPartitionPerSplit() {
    return partitionPerSplit;
  }

  public void setPartitionPerSplit(int partitionPerSplit) {
    this.partitionPerSplit = partitionPerSplit;
  }

  public List<TiStoreType> getIsolationReadEngines() {
    return isolationReadEngines;
  }

  public void setIsolationReadEngines(List<TiStoreType> isolationReadEngines) {
    this.isolationReadEngines = isolationReadEngines;
  }

  public int getKvClientConcurrency() {
    return kvClientConcurrency;
  }

  public void setKvClientConcurrency(int kvClientConcurrency) {
    this.kvClientConcurrency = kvClientConcurrency;
  }
}
