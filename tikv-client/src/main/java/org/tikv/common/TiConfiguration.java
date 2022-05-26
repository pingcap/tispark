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

package org.tikv.common;

import static org.tikv.common.ConfigUtils.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ConfigUtils.*;
import org.tikv.common.pd.PDUtils;
import org.tikv.common.region.TiStoreType;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

@Getter
@Setter
@Accessors(chain = true)
public class TiConfiguration implements Serializable {
  private static final Logger logger = LoggerFactory.getLogger(TiConfiguration.class);
  private static final ConcurrentHashMap<String, String> settings = new ConcurrentHashMap<>();

  static {
    loadFromSystemProperties();
    loadFromConfigurationFile();
    loadFromDefaultProperties();
    listAll();
  }

  public static void listAll() {
    logger.info("static configurations are:" + new ArrayList<>(settings.entrySet()).toString());
  }

  private static void loadFromSystemProperties() {
    for (Map.Entry<String, String> prop : Utils.getSystemProperties().entrySet()) {
      if (prop.getKey().startsWith("tikv.")) {
        set(prop.getKey(), prop.getValue());
      }
    }
  }

  private static void loadFromConfigurationFile() {
    try (InputStream input =
        TiConfiguration.class
            .getClassLoader()
            .getResourceAsStream(ConfigUtils.TIKV_CONFIGURATION_FILENAME)) {
      Properties properties = new Properties();

      if (input == null) {
        logger.warn("Unable to find " + ConfigUtils.TIKV_CONFIGURATION_FILENAME);
        return;
      }

      logger.info("loading " + ConfigUtils.TIKV_CONFIGURATION_FILENAME);
      properties.load(input);
      for (String key : properties.stringPropertyNames()) {
        if (key.startsWith("tikv.")) {
          String value = properties.getProperty(key);
          setIfMissing(key, value);
        }
      }
    } catch (IOException e) {
      logger.error("load config file error", e);
    }
  }

  private static void loadFromDefaultProperties() {
    setIfMissing(TIKV_TIMEZONE, String.valueOf(DEF_TIMEZONE));
    setIfMissing(TIKV_TIMEOUT, DEF_TIMEOUT);
    setIfMissing(TIKV_TIMEOUT_UNIT, String.valueOf(DEF_TIMEOUT_UNIT));
    setIfMissing(TIKV_SCAN_BATCH_SIZE, DEF_SCAN_BATCH_SIZE);
    setIfMissing(TIKV_IGNORE_TRUNCATE, DEF_IGNORE_TRUNCATE);
    setIfMissing(TIKV_TRUNCATE_AS_WARNING, DEF_TRUNCATE_AS_WARNING);
    setIfMissing(TIKV_MAX_FRAME_SIZE, DEF_MAX_FRAME_SIZE);
    setIfMissing(TIKV_INDEX_SCAN_BATCH_SIZE, DEF_INDEX_SCAN_BATCH_SIZE);
    setIfMissing(TIKV_REGION_SCAN_DOWNGRADE_THRESHOLD, DEF_REGION_SCAN_DOWNGRADE_THRESHOLD);
    setIfMissing(TIKV_MAX_REQUEST_KEY_RANGE_SIZE, MAX_REQUEST_KEY_RANGE_SIZE);
    setIfMissing(TIKV_INDEX_SCAN_CONCURRENCY, DEF_INDEX_SCAN_CONCURRENCY);
    setIfMissing(TIKV_TABLE_SCAN_CONCURRENCY, DEF_TABLE_SCAN_CONCURRENCY);
    setIfMissing(TIKV_BATCH_GET_CONCURRENCY, DEF_BATCH_GET_CONCURRENCY);
    setIfMissing(TIKV_BATCH_PUT_CONCURRENCY, DEF_BATCH_PUT_CONCURRENCY);
    setIfMissing(TIKV_BATCH_DELETE_CONCURRENCY, DEF_BATCH_DELETE_CONCURRENCY);
    setIfMissing(TIKV_BATCH_SCAN_CONCURRENCY, DEF_BATCH_SCAN_CONCURRENCY);

    setIfMissing(TIKV_DELETE_RANGE_CONCURRENCY, DEF_DELETE_RANGE_CONCURRENCY);

    setIfMissing(TIKV_COMMAND_PRIORITY, String.valueOf(DEF_COMMAND_PRIORITY));
    setIfMissing(TIKV_ISOLATION_LEVEL, String.valueOf(DEF_ISOLATION_LEVEL));
    setIfMissing(TIKV_SHOW_ROWID, DEF_SHOW_ROWID);

    setIfMissing(TIKV_DB_PREFIX, DEF_DB_PREFIX);
    setIfMissing(TIKV_WRITE_ENABLE, DEF_WRITE_ENABLE);
    setIfMissing(TIKV_WRITE_ALLOW_SPARK_SQL, DEF_WRITE_ALLOW_SPARK_SQL);

    setIfMissing(TIKV_WRITE_WITHOUT_LOCK_TABLE, DEF_WRITE_WITHOUT_LOCK_TABLE);
    setIfMissing(TIKV_REGION_SPLIT_SIZE_IN_MB, DEF_TIKV_REGION_SPLIT_SIZE_IN_MB);
    setIfMissing(TIKV_PARTITION_PER_SPLIT, DEF_PARTITION_PER_SPLIT);
    setIfMissing(TIKV_KV_CLIENT_CONCURRENCY, DEF_KV_CLIENT_CONCURRENCY);

    setIfMissing(TIKV_KV_CLIENT_CONCURRENCY, DEF_KV_CLIENT_CONCURRENCY);
  }

  private static void setIfMissing(String key, int value) {
    setIfMissing(key, String.valueOf(value));
  }

  private static void setIfMissing(String key, boolean value) {
    setIfMissing(key, String.valueOf(value));
  }

  private static void setIfMissing(String key, String value) {
    if (key == null) {
      throw new NullPointerException("null key");
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key);
    }
    settings.putIfAbsent(key, value);
  }

  private static void set(String key, String value) {
    if (key == null) {
      throw new NullPointerException("null key");
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key);
    }
    settings.put(key, value);
  }

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
}
