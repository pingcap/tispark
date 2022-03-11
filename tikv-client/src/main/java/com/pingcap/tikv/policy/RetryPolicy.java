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
import com.pingcap.tikv.operation.ErrorHandler;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import io.grpc.Status;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;

public abstract class RetryPolicy<RespT> {
  // handles PD and TiKV's error.
  private final ErrorHandler<RespT> handler;
  private final ImmutableSet<Status.Code> unrecoverableStatus =
      ImmutableSet.of(
          Status.Code.ALREADY_EXISTS, Status.Code.PERMISSION_DENIED,
          Status.Code.INVALID_ARGUMENT, Status.Code.NOT_FOUND,
          Status.Code.UNIMPLEMENTED, Status.Code.OUT_OF_RANGE,
          Status.Code.UNAUTHENTICATED, Status.Code.CANCELLED);
  BackOffer backOffer = ConcreteBackOffer.newCopNextMaxBackOff();

  RetryPolicy(@Nonnull ErrorHandler<RespT> handler) {
    this.handler = handler;
  }

  private void rethrowNotRecoverableException(Exception e) throws GrpcException {
    Status status = Status.fromThrowable(e);
    if (unrecoverableStatus.contains(status.getCode())) {
      throw new GrpcException(e);
    }
  }

  public RespT callWithRetry(Callable<RespT> proc, String methodName) throws GrpcException {
    while (true) {
      RespT result = null;
      try {
        result = proc.call();
      } catch (Exception e) {
        rethrowNotRecoverableException(e);
        // Handle request call error
        boolean retry = handler.handleRequestError(backOffer, e);
        if (retry) {
          continue;
        }
      }

      // Handle response error
      if (result != null) {
        boolean retry = handler.handleResponseError(backOffer, result);
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
