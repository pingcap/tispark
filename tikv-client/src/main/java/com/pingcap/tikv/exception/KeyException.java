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

package com.pingcap.tikv.exception;

import org.tikv.kvproto.Kvrpcpb;

public class KeyException extends TiKVException {

  private static final long serialVersionUID = 6649195220216182286L;
  private Kvrpcpb.KeyError keyError;

  public KeyException(String errMsg) {
    super(errMsg);
  }

  public KeyException(Kvrpcpb.KeyError keyErr) {
    super(String.format("Key exception occurred and the reason is %s", keyErr.toString()));
    this.keyError = keyErr;
  }

  public KeyException(Kvrpcpb.KeyError keyErr, String errMsg) {
    super(errMsg);
    this.keyError = keyErr;
  }

  public Kvrpcpb.KeyError getKeyError() {
    return keyError;
  }
}
