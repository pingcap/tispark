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

import com.pingcap.tikv.util.BackOffer;

public interface ErrorHandler<RespT> {
  /**
   * Handle the error received in the response after a calling process completes.
   *
   * @param backOffer Back offer used for retry
   * @param resp the response to handle
   * @return whether the caller should retry
   */
  boolean handleResponseError(BackOffer backOffer, RespT resp);

  /**
   * Handle the error received during a calling process before it could return a normal response.
   *
   * @param backOffer Back offer used for retry
   * @param e Exception received during a calling process
   * @return whether the caller should retry
   */
  boolean handleRequestError(BackOffer backOffer, Exception e);
}
