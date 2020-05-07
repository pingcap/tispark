/*
 *
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tikv.txn;

public class TxnExpireTime {

  private boolean initialized = false;
  private long txnExpire = 0;

  public TxnExpireTime() {}

  public TxnExpireTime(boolean initialized, long txnExpire) {
    this.initialized = initialized;
    this.txnExpire = txnExpire;
  }

  public void update(long lockExpire) {
    if (lockExpire < 0) {
      lockExpire = 0;
    }

    if (!this.initialized) {
      this.txnExpire = lockExpire;
      this.initialized = true;
      return;
    }

    if (lockExpire < this.txnExpire) {
      this.txnExpire = lockExpire;
    }
  }

  public long value() {
    if (!this.initialized) {
      return 0L;
    } else {
      return this.txnExpire;
    }
  }
}
