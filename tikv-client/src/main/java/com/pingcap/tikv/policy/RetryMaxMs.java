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

package com.pingcap.tikv.policy;

import com.pingcap.tikv.operation.ErrorHandler;
import com.pingcap.tikv.util.BackOffer;

public class RetryMaxMs<T> extends RetryPolicy<T> {
  private RetryMaxMs(ErrorHandler<T> handler, BackOffer backOffer) {
    super(handler);
    this.backOffer = backOffer;
  }

  public static class Builder<T> implements RetryPolicy.Builder<T> {
    private final BackOffer backOffer;

    public Builder(BackOffer backOffer) {
      this.backOffer = backOffer;
    }

    @Override
    public RetryPolicy<T> create(ErrorHandler<T> handler) {
      return new RetryMaxMs<>(handler, backOffer);
    }
  }
}
