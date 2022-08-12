/*
 * Copyright 2022 PingCAP, Inc.
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

package com.pingcap.tikv.util;

import java.util.Objects;
import java.util.stream.Collectors;

/** Convert some classes that are different from upstream. */
public class ConvertUpstreamUtils {

  public static org.tikv.common.TiConfiguration convertTiConfiguration(
      com.pingcap.tikv.TiConfiguration conf) {
    org.tikv.common.TiConfiguration tikvConf;
    if (conf == null) {
      tikvConf = org.tikv.common.TiConfiguration.createDefault();
    } else {
      tikvConf =
          org.tikv.common.TiConfiguration.createDefault(
              conf.getPdAddrs().stream().map(Objects::toString).collect(Collectors.joining(",")));
    }

    //    can't set
    // tikvConf.setNetWorkMappingName(conf.getNetworkMappingName());// same default value
    // tikvConf.setDownGradeThreshold(conf.getDowngradeThreshold());//same default value
    // tikvConf.setMaxRequestKeyRangeSize(conf.getMaxRequestKeyRangeSize());//same default value

    // different
    //   timeout
    //       tispark      10  TimeUnit.MINUTES
    //       client-java  200 TimeUnit.MILLISECONDS
    tikvConf.setTimeout(conf.getTimeoutUnit().toMillis(conf.getTimeout()));
    // maxFrameSize
    //         tispark      2GB
    //         client-java  512MB
    tikvConf.setMaxFrameSize(conf.getMaxFrameSize());

    // same
    tikvConf.setHostMapping(conf.getHostMapping());
    tikvConf.setIndexScanBatchSize(conf.getIndexScanBatchSize());
    tikvConf.setIndexScanConcurrency(conf.getIndexScanConcurrency());
    tikvConf.setTableScanConcurrency(conf.getTableScanConcurrency());
    tikvConf.setKvClientConcurrency(conf.getKvClientConcurrency());
    tikvConf.setConnRecycleTimeInSeconds(Math.toIntExact(conf.getConnRecycleTime()));
    tikvConf.setCertReloadIntervalInSeconds(conf.getCertReloadInterval());
    tikvConf.setBatchGetConcurrency(conf.getBatchGetConcurrency());
    tikvConf.setBatchPutConcurrency(conf.getBatchPutConcurrency());
    tikvConf.setBatchDeleteConcurrency(conf.getBatchDeleteConcurrency());
    tikvConf.setBatchScanConcurrency(conf.getBatchScanConcurrency());
    tikvConf.setDeleteRangeConcurrency(conf.getDeleteRangeConcurrency());
    tikvConf.setCommandPriority(conf.getCommandPriority());
    tikvConf.setIsolationLevel(conf.getIsolationLevel());
    tikvConf.setShowRowId(conf.isShowRowId());
    tikvConf.setDBPrefix(conf.getDBPrefix());
    // jks
    tikvConf.setTlsEnable(conf.isTlsEnable());
    tikvConf.setTrustCertCollectionFile(conf.getTrustCertCollectionFile());
    tikvConf.setKeyCertChainFile(conf.getKeyCertChainFile());
    tikvConf.setKeyFile(conf.getKeyFile());
    tikvConf.setJksKeyPath(conf.getJksKeyPath());
    tikvConf.setJksKeyPassword(conf.getJksKeyPassword());
    tikvConf.setJksTrustPath(conf.getJksTrustPath());
    tikvConf.setJksTrustPassword(conf.getJksTrustPassword());
    tikvConf.setJksEnable(conf.isJksEnable());
    return tikvConf;
  }
}
