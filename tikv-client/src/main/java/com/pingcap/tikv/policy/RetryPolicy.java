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

import com.google.common.collect.ImmutableSet;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.GrpcNeedRegionRefreshException;
import com.pingcap.tikv.operation.ErrorHandler;
import com.pingcap.tikv.util.BackOff;
import com.pingcap.tikv.util.BackoffFunction;
import io.grpc.Status;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public abstract class RetryPolicy<RespT> {
  private static final Logger logger = Logger.getLogger(RetryPolicy.class);

  BackOff backOff = BackOff.ZERO_BACKOFF;

  // handles PD and TiKV's error.
  private ErrorHandler<RespT> handler;

  private ImmutableSet<Status.Code> unrecoverableStatus =
      ImmutableSet.of(
          Status.Code.ALREADY_EXISTS, Status.Code.PERMISSION_DENIED,
          Status.Code.INVALID_ARGUMENT, Status.Code.NOT_FOUND,
          Status.Code.UNIMPLEMENTED, Status.Code.OUT_OF_RANGE,
          Status.Code.UNAUTHENTICATED, Status.Code.CANCELLED);

  RetryPolicy(ErrorHandler<RespT> handler) {
    this.handler = handler;
  }

  private void rethrowNotRecoverableException(Exception e) {
    if (e instanceof GrpcNeedRegionRefreshException) {
      throw (GrpcNeedRegionRefreshException) e;
    }
    Status status = Status.fromThrowable(e);
    if (unrecoverableStatus.contains(status.getCode())) {
      throw new GrpcException(e);
    }
  }

  private void handleFailure(Exception e, String methodName, long nextBackMills) {
    if (nextBackMills == BackOff.STOP) {
      throw new GrpcException("retry is exhausted.", e);
    }
    rethrowNotRecoverableException(e);
    doWait(nextBackMills);
  }

  private void doWait(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new GrpcException(e);
    }
  }

  public RespT callWithRetry(Callable<RespT> proc, String methodName) {
    while (true) {
      RespT result = null;
      try {
        result = proc.call();
      } catch (Exception e) {
        // Handle request call error
        boolean retry =  handler.handleRequestError(backOff, e);
        if (retry) {
          continue;
        }
      }

      // Handle response error
      if (handler != null) {
        boolean retry = handler.handleResponseError(backOff, result);
        if (retry) {
          continue;
        }
      }
      return result;
    }
  }

  public interface Builder<T> {
    RetryPolicy<T> create(ErrorHandler<T> handler);
  }
}
