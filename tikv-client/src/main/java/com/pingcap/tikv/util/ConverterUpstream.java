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
import org.tikv.common.TiConfiguration.ApiVersion;
import org.tikv.common.apiversion.RequestKeyCodec;

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
    if (conf == null) {
      return null;
    }
    org.tikv.common.TiConfiguration tikvConf =
        org.tikv.common.TiConfiguration.createDefault(
            conf.getPdAddrs().stream().map(Objects::toString).collect(Collectors.joining(",")));
    // s --> ms
    tikvConf.setTimeout(conf.getTimeout() * 1000L);
    tikvConf.setKvClientConcurrency(conf.getKvClientConcurrency());
    tikvConf.setIsolationLevel(conf.getIsolationLevel());
    tikvConf.setCommandPriority(conf.getCommandPriority());
    tikvConf.setApiVersion(ApiVersion.V1);
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

  public static org.tikv.common.util.BackOffFunction.BackOffFuncType convertBackOffFunctionType(
      com.pingcap.tikv.util.BackOffFunction.BackOffFuncType funcType) {
    return org.tikv.common.util.BackOffFunction.BackOffFuncType.valueOf(funcType.name());
  }
}
