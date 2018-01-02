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

import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.operation.ErrorHandler;
import com.pingcap.tikv.util.BackOff;

public class RetryNTimes<T> extends RetryPolicy<T> {
  private RetryNTimes(ErrorHandler<T> handler, BackOff backOff) {
    super(handler);
    this.backOff = backOff;
  }

  public static class Builder<T> implements RetryPolicy.Builder<T> {
    private BackOff backOff;

    public Builder(int n, Class<? extends BackOff> backoffClass) {
      try {
        this.backOff = backoffClass.getConstructor(int.class).newInstance(n);
      } catch (Exception e) {
        throw new TiClientInternalException("failed to toIndexKey backoff object", e);
      }
    }

    @Override
    public RetryPolicy<T> create(ErrorHandler<T> handler) {
      return new RetryNTimes<>(handler, backOff);
    }
  }
}
