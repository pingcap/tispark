package com.pingcap.tikv.region;

public enum TiStoreType {
  TiKV,
  TiFlash;

  private static final String TIFLASH_LABEL_KEY = "engine";
  private static final String TIFLASH_LABEL_VALUE = "tiflash";

  static {
    TiFlash.labelKey = TIFLASH_LABEL_KEY;
    TiFlash.labelValue = TIFLASH_LABEL_VALUE;
  }

  @Override
  public String toString() {
    if (labelKey == null || labelValue == null) return name();
    return name() + "(" + labelKey + "=" + labelValue + ")";
  }

  public String getLabelKey() {
    return labelKey;
  }

  public String getLabelValue() {
    return labelValue;
  }

  private String labelKey = null;
  private String labelValue = null;
}
