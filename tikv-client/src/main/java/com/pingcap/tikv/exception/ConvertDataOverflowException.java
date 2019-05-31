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

public class ConvertDataOverflowException extends RuntimeException {
  private ConvertDataOverflowException(String msg) {
    super(msg);
  }

  public static ConvertDataOverflowException newLowerBound(Object value, Object lowerBound) {
    return new ConvertDataOverflowException("value " + value + " < lowerBound " + lowerBound);
  }

  public static ConvertDataOverflowException newUpperBound(Object value, Object upperBound) {
    return new ConvertDataOverflowException("value " + value + " > upperBound " + upperBound);
  }
}
