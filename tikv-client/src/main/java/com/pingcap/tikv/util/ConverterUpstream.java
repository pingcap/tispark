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
import org.tikv.common.apiversion.RequestKeyCodec;

/**
 * Encapsulate some the parameters that we maintain that are different from upstream into upstream
 * library parameters to be consistent with the parameter types maintained everywhere. Upstream
 * parameters are also converted to downstream parameters when needed. Distinguish convert/reConvert
 * by name (downstream converts to upstream, upstream converts to downstream).
 */
public class ConverterUpstream {

  public static org.tikv.common.ReadOnlyPDClient createUpstreamPDClient(
      com.pingcap.tikv.TiConfiguration conf, RequestKeyCodec keyCodec) {
    org.tikv.common.TiConfiguration tikvConf = ConverterUpstream.convertTiConfiguration(conf);
    org.tikv.common.util.ChannelFactory channelFactory =
        ConverterUpstream.convertChannelFactory(conf, tikvConf);
    return org.tikv.common.PDClient.create(tikvConf, keyCodec, channelFactory);
  }

  public static org.tikv.common.TiConfiguration convertTiConfiguration(
      com.pingcap.tikv.TiConfiguration conf) {
    org.tikv.common.TiConfiguration tikvConf;

    // pdaddrs
    // tispark --- null
    // client-java -- "127.0.0.1:2379"
    if (conf == null) {
      tikvConf = org.tikv.common.TiConfiguration.createDefault();
    } else {
      tikvConf =
          org.tikv.common.TiConfiguration.createDefault(
              conf.getPdAddrs().stream().map(Objects::toString).collect(Collectors.joining(",")));
    }

    //   timeout
    //       tispark 10 TimeUnit.MINUTES
    //       client-java  200 TimeUnit.MILLISECONDS
    // s --> ms
    tikvConf.setTimeout(conf.getTimeoutUnit().toMillis(conf.getTimeout()));

    // maxFrameSize
    //        tispark 2GB
    //         client-java 512MB
    tikvConf.setMaxFrameSize(conf.getMaxFrameSize());

    // ScanBatchSize
    //      tispark-- 10480
    //      client-java --10240
    //    can't set
    //        tikvConf.setScanBathSize(conf.getScanBatchSize());

    // same
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
    //    tikvConf.setNetworkMappingName(conf.getNetworkMappingName());
    tikvConf.setHostMapping(conf.getHostMapping());
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

  public static org.tikv.common.util.ChannelFactory convertChannelFactory(
      com.pingcap.tikv.TiConfiguration conf, org.tikv.common.TiConfiguration tikvConf) {
    org.tikv.common.util.ChannelFactory tikvFactory = null;
    if (conf.isTlsEnable()) {
      if (conf.isJksEnable()) {
        tikvFactory =
            new org.tikv.common.util.ChannelFactory(
                conf.getMaxFrameSize(),
                tikvConf.getKeepaliveTime(),
                tikvConf.getKeepaliveTimeout(),
                tikvConf.getIdleTimeout(),
                conf.getConnRecycleTime(),
                conf.getCertReloadInterval(),
                conf.getJksKeyPath(),
                conf.getJksKeyPassword(),
                conf.getJksTrustPath(),
                conf.getJksTrustPassword());
      } else {
        tikvFactory =
            new org.tikv.common.util.ChannelFactory(
                conf.getMaxFrameSize(),
                tikvConf.getKeepaliveTime(),
                tikvConf.getKeepaliveTimeout(),
                tikvConf.getIdleTimeout(),
                conf.getConnRecycleTime(),
                conf.getCertReloadInterval(),
                conf.getTrustCertCollectionFile(),
                conf.getKeyCertChainFile(),
                conf.getKeyFile());
      }
    } else {
      tikvFactory =
          new org.tikv.common.util.ChannelFactory(
              conf.getMaxFrameSize(),
              tikvConf.getKeepaliveTime(),
              tikvConf.getKeepaliveTimeout(),
              tikvConf.getIdleTimeout());
    }
    return tikvFactory;
  }
}
