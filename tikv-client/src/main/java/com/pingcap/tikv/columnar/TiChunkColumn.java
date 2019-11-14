package com.pingcap.tikv.columnar;

import com.pingcap.tikv.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public abstract class TiChunkColumn {
  protected DataType dataType;
  protected int size;

  public TiChunkColumn(DataType dataType, int size) {
    this.dataType = dataType;
    this.size = size;
  }

  public final DataType dataType() {
    return dataType;
  }

  public final String typeName() {
    return dataType().getType().name();
  }

  public int size() {
    return size;
  }

  public abstract long byteCount();

  public abstract void free();

  public boolean empty() {
    return size() == 0;
  }

  public boolean isNullAt(int rowId) {
    return false;
  }

  public byte getByte(int rowId) {
    throw new UnsupportedOperationException();
  }

  public short getShort(int rowId) {
    throw new UnsupportedOperationException();
  }

  public int getInt(int rowId) {
    throw new UnsupportedOperationException();
  }

  public long getLong(int rowId) {
    throw new UnsupportedOperationException();
  }

  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  public Decimal getDecimal(int rowId) {
    throw new UnsupportedOperationException();
  }

  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  public void insertDefault() {
    size++;
  }

  public void insertNull() {
    throw new UnsupportedOperationException();
  }

  public void insertByte(byte v) {
    throw new UnsupportedOperationException();
  }

  public void insertShort(short v) {
    throw new UnsupportedOperationException();
  }

  public void insertInt(int v) {
    throw new UnsupportedOperationException();
  }

  public void insertLong(long v) {
    throw new UnsupportedOperationException();
  }

  public void insertFloat(float v) {
    throw new UnsupportedOperationException();
  }

  public void insertDouble(double v) {
    throw new UnsupportedOperationException();
  }

  public void insertDecimal(Decimal v) {
    throw new UnsupportedOperationException();
  }

  public void insertUTF8String(UTF8String v) {
    throw new UnsupportedOperationException();
  }

  /**
   * After done with insertion, you should call this method to make the inserted data readable.
   * Mainly used to avoid frequently reallocating memory.
   */
  public abstract TiChunkColumn seal();
}
