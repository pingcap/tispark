package com.pingcap.tikv.exception;

public class NonAsyncCommitLockException extends RuntimeException {

  public NonAsyncCommitLockException(String msg) {
    super(msg);
  }
}
