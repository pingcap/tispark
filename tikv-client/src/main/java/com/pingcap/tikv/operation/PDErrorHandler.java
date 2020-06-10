/*
 *
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
 *
 */

package com.pingcap.tikv.operation;

import static com.pingcap.tikv.pd.PDError.buildFromPdpbError;

import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.pd.PDError;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Pdpb;

public class PDErrorHandler<RespT> implements ErrorHandler<RespT> {
  public static final Function<Pdpb.GetRegionResponse, PDError> getRegionResponseErrorExtractor =
      r ->
          r.getHeader().hasError()
              ? buildFromPdpbError(r.getHeader().getError())
              : r.getRegion().getId() == 0 ? PDError.RegionPeerNotElected.DEFAULT_INSTANCE : null;
  private static final Logger logger = LoggerFactory.getLogger(PDErrorHandler.class);
  private final Function<RespT, PDError> getError;
  private final PDClient client;

  public PDErrorHandler(Function<RespT, PDError> errorExtractor, PDClient client) {
    this.getError = errorExtractor;
    this.client = client;
  }

  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    if (resp == null) {
      return false;
    }
    PDError error = getError.apply(resp);
    if (error != null) {
      switch (error.getErrorType()) {
        case PD_ERROR:
          backOffer.doBackOff(
              BackOffFunction.BackOffFuncType.BoPDRPC, new GrpcException(error.toString()));
          client.updateLeader();
          return true;
        case REGION_PEER_NOT_ELECTED:
          logger.debug(error.getMessage());
          backOffer.doBackOff(
              BackOffFunction.BackOffFuncType.BoPDRPC, new GrpcException(error.toString()));
          return true;
        default:
          throw new TiClientInternalException("Unknown error type encountered: " + error);
      }
    }
    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoPDRPC, e);
    return true;
  }
}
