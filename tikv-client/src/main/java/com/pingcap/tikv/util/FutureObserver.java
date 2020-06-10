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

package com.pingcap.tikv.util;

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Future;

public class FutureObserver<Value, RespT> implements StreamObserver<RespT> {
  private final SettableFuture<Value> resultFuture;
  private final Getter<Value, RespT> getter;

  public FutureObserver(Getter<Value, RespT> getter) {
    this.resultFuture = SettableFuture.create();
    this.getter = getter;
  }

  public Value getValue(RespT resp) {
    return getter.getValue(resp);
  }

  @Override
  public void onNext(RespT resp) {
    resultFuture.set(getValue(resp));
  }

  @Override
  public void onError(Throwable t) {
    resultFuture.setException(t);
  }

  @Override
  public void onCompleted() {}

  public Future<Value> getFuture() {
    return resultFuture;
  }

  public interface Getter<Value, RespT> {
    Value getValue(RespT resp);
  }
}
