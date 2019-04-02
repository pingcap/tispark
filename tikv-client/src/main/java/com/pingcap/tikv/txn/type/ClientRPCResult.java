/*
 * Copyright 2019 The TiKV Project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.txn.type;

public class ClientRPCResult {
  private boolean success;
  private boolean retry;
  private String error;

  public ClientRPCResult(boolean success, boolean retry, String error) {
    this.success = success;
    this.retry = retry;
    this.error = error;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public boolean isRetry() {
    return retry;
  }

  public void setRetry(boolean retry) {
    this.retry = retry;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }
}
