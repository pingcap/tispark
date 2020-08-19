/*
 * Copyright 2019 PingCAP, Inc.
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

public class ConvertOverflowException extends RuntimeException {
  private ConvertOverflowException(String msg) {
    super(msg);
  }

  public ConvertOverflowException(String msg, Throwable e) {
    super(msg, e);
  }

  public static ConvertOverflowException newMaxLengthException(String value, long maxLength) {
    return new ConvertOverflowException("value " + value + " length > max length " + maxLength);
  }

  public static ConvertOverflowException newMaxLengthException(long length, long maxLength) {
    return new ConvertOverflowException("length " + length + " > max length " + maxLength);
  }

  public static ConvertOverflowException newLowerBoundException(Object value, Object lowerBound) {
    return new ConvertOverflowException("value " + value + " < lowerBound " + lowerBound);
  }

  public static ConvertOverflowException newUpperBoundException(Object value, Object upperBound) {
    return new ConvertOverflowException("value " + value + " > upperBound " + upperBound);
  }

  public static ConvertOverflowException newEnumException(Object value) {
    return new ConvertOverflowException("Incorrect enum value: '" + value + "'");
  }

  public static ConvertOverflowException newOutOfRange() {
    return new ConvertOverflowException("Out of range");
  }
}
