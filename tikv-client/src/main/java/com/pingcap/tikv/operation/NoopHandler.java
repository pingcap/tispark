package com.pingcap.tikv.operation;

import com.pingcap.tikv.util.BackOffer;

public class NoopHandler<RespT> implements ErrorHandler<RespT> {

  @Override
  public boolean handleResponseError(BackOffer backOffer, Object resp) {
    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    return false;
  }
}
