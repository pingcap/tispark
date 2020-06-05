/*
 *
 * Copyright 2018 PingCAP, Inc.
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

package com.pingcap.tikv.pd;

import org.tikv.kvproto.Pdpb;

public final class PDError {
  private final Pdpb.Error error;

  private final ErrorType errorType;

  private PDError(Pdpb.Error error) {
    this.error = error;
    this.errorType = ErrorType.PD_ERROR;
  }

  private PDError(Pdpb.Error error, ErrorType errorType) {
    this.error = error;
    this.errorType = errorType;
  }

  public static PDError buildFromPdpbError(Pdpb.Error error) {
    return new PDError(error);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(Pdpb.Error error) {
    return new Builder(error);
  }

  public Pdpb.Error getError() {
    return error;
  }

  public ErrorType getErrorType() {
    return errorType;
  }

  public String getMessage() {
    return getError().getMessage();
  }

  @Override
  public String toString() {
    return "\nErrorType: " + errorType + "\nError: " + error;
  }

  public enum ErrorType {
    PD_ERROR,
    REGION_PEER_NOT_ELECTED
  }

  public static final class RegionPeerNotElected {
    private static final String ERROR_MESSAGE = "Region Peer not elected. Please try later";
    private static final Pdpb.Error DEFAULT_ERROR =
        Pdpb.Error.newBuilder().setMessage(ERROR_MESSAGE).build();
    private static final ErrorType ERROR_TYPE = ErrorType.REGION_PEER_NOT_ELECTED;
    public static final PDError DEFAULT_INSTANCE =
        PDError.newBuilder(DEFAULT_ERROR).setErrorType(ERROR_TYPE).build();
  }

  public static final class Builder {
    private Pdpb.Error error_;
    private ErrorType errorType_ = ErrorType.PD_ERROR;

    public Builder() {}

    public Builder(Pdpb.Error error) {
      this.error_ = error;
    }

    public Builder setError(Pdpb.Error error) {
      this.error_ = error;
      return this;
    }

    public Builder setErrorType(ErrorType errorType) {
      this.errorType_ = errorType;
      return this;
    }

    public PDError build() {
      return new PDError(error_, errorType_);
    }
  }
}
