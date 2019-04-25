package com.pingcap.tikv.exception;

public class UnsupportedPartitionExprException extends RuntimeException {
  public UnsupportedPartitionExprException(String msg) {
    super(msg);
  }
}
