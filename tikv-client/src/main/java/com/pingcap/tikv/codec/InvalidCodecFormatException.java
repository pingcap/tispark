package com.pingcap.tikv.codec;

public class InvalidCodecFormatException extends RuntimeException {
  public InvalidCodecFormatException(String msg) {
    super(msg);
  }
}
