package com.pingcap.tikv.util;

import com.pingcap.tikv.exception.GrpcException;

import java.util.concurrent.ThreadLocalRandom;

public class BackoffFunction {
  private int base;
  private int cap;
  private int lastSleep;
  private int attempts;
  private BackOff.BackOffStrategy strategy;

  public static BackoffFunction create(int base, int cap, BackOff.BackOffStrategy strategy) {
    return new BackoffFunction(base, cap, strategy);
  }

  private BackoffFunction(int base, int cap, BackOff.BackOffStrategy strategy) {
    this.base = base;
    this.cap = cap;
    this.strategy = strategy;
    lastSleep = base;
  }

  public enum BackOffFuncType {
    boTiKVRPC,
    BoTxnLock,
    boTxnLockFast,
    boPDRPC,
    BoRegionMiss,
    BoUpdateLeader,
    boServerBusy
  }

  public int doBackOff() {
    int sleep = 0;
    int v = expo(base, cap, attempts);
    switch (strategy) {
      case NoJitter:
        sleep = v;
        break;
      case FullJitter:
        sleep = ThreadLocalRandom.current().nextInt(v);
        break;
      case EqualJitter:
        sleep = v / 2 + ThreadLocalRandom.current().nextInt(v / 2);
        break;
      case DecorrJitter:
        sleep = Math.min(cap, base + ThreadLocalRandom.current().nextInt(lastSleep * 3 - base));
        break;
    }

    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      throw new GrpcException(e);
    }
    attempts++;
    lastSleep = sleep;
    return lastSleep;
  }

  private int expo(int base, int cap, int n) {
    return (int) Math.min(cap, base * Math.pow(2.0d, n));
  }
}
