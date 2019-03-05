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
import com.pingcap.tikv.kvproto.Kvrpcpb.CommandPri;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TiConfiguration implements Serializable {
  private static final int DEF_TIMEOUT = 10;
  private static final TimeUnit DEF_TIMEOUT_UNIT = TimeUnit.MINUTES;
  private static final int DEF_SCAN_BATCH_SIZE = 100;
  private static final boolean DEF_IGNORE_TRUNCATE = true;
  private static final boolean DEF_TRUNCATE_AS_WARNING = false;
  private static final int DEF_META_RELOAD_PERIOD = 10;
  private static final TimeUnit DEF_META_RELOAD_UNIT = TimeUnit.SECONDS;
  private static final int DEF_MAX_FRAME_SIZE = 268435456 * 2; // 256 * 2 MB
  private static final int DEF_INDEX_SCAN_BATCH_SIZE = 20000;
  // if keyRange size per request exceeds this limit, the request might be too large to be accepted
  // by TiKV(maximum request size accepted by TiKV is around 1MB)
  private static final int MAX_REQUEST_KEY_RANGE_SIZE = 20000;
  private static final int DEF_INDEX_SCAN_CONCURRENCY = 5;
  private static final int DEF_TABLE_SCAN_CONCURRENCY = 512;
  private static final CommandPri DEF_COMMAND_PRIORITY = CommandPri.Low;
  private static final IsolationLevel DEF_ISOLATION_LEVEL = IsolationLevel.RC;
  private static final boolean DEF_SHOW_ROWID = false;
  private static final String DEF_DB_PREFIX = "";

  private int timeout = DEF_TIMEOUT;
  private TimeUnit timeoutUnit = DEF_TIMEOUT_UNIT;
  private boolean ignoreTruncate = DEF_IGNORE_TRUNCATE;
  private boolean truncateAsWarning = DEF_TRUNCATE_AS_WARNING;
  private TimeUnit metaReloadUnit = DEF_META_RELOAD_UNIT;
  private int metaReloadPeriod = DEF_META_RELOAD_PERIOD;
  private int maxFrameSize = DEF_MAX_FRAME_SIZE;
  private List<URI> pdAddrs = new ArrayList<>();
  private int indexScanBatchSize = DEF_INDEX_SCAN_BATCH_SIZE;
  private int indexScanConcurrency = DEF_INDEX_SCAN_CONCURRENCY;
  private int tableScanConcurrency = DEF_TABLE_SCAN_CONCURRENCY;
  private CommandPri commandPriority = DEF_COMMAND_PRIORITY;
  private IsolationLevel isolationLevel = DEF_ISOLATION_LEVEL;
  private int maxRequestKeyRangeSize = MAX_REQUEST_KEY_RANGE_SIZE;
  private boolean showRowId = DEF_SHOW_ROWID;
  private String dbPrefix = DEF_DB_PREFIX;

  public static TiConfiguration createDefault(String pdAddrsStr) {
    Objects.requireNonNull(pdAddrsStr, "pdAddrsStr is null");
    TiConfiguration conf = new TiConfiguration();
    conf.pdAddrs = strToURI(pdAddrsStr);
    return conf;
  }

  private static List<URI> strToURI(String addressStr) {
    Objects.requireNonNull(addressStr);
    String[] addrs = addressStr.split(",");
    ImmutableList.Builder<URI> addrsBuilder = ImmutableList.builder();
    for (String addr : addrs) {
      addrsBuilder.add(URI.create("http://" + addr));
    }
    return addrsBuilder.build();
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

  public TimeUnit getMetaReloadPeriodUnit() {
    return metaReloadUnit;
  }

  public TiConfiguration setMetaReloadPeriodUnit(TimeUnit timeUnit) {
    this.metaReloadUnit = timeUnit;
    return this;
  }

  public TiConfiguration setMetaReloadPeriod(int metaReloadPeriod) {
    this.metaReloadPeriod = metaReloadPeriod;
    return this;
  }

  public int getMetaReloadPeriod() {
    return metaReloadPeriod;
  }

  public TiConfiguration setTimeoutUnit(TimeUnit timeoutUnit) {
    this.timeoutUnit = timeoutUnit;
    return this;
  }

  public List<URI> getPdAddrs() {
    return pdAddrs;
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
}
