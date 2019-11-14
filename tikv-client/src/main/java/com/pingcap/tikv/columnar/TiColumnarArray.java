package com.pingcap.tikv.columnar;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/** Array abstraction in {@link ColumnVector}. */
@InterfaceStability.Evolving
public final class TiColumnarArray extends ArrayData {
  // The data for this array. This array contains elements from
  // data[offset] to data[offset + length).
  private final TiColumnVector data;
  private final int offset;
  private final int length;

  public TiColumnarArray(TiColumnVector data, int offset, int length) {
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int numElements() {
    return length;
  }

  @Override
  public ArrayData copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean[] toBooleanArray() {
    return data.getBooleans(offset, length);
  }

  @Override
  public byte[] toByteArray() {
    return data.getBytes(offset, length);
  }

  @Override
  public short[] toShortArray() {
    return data.getShorts(offset, length);
  }

  @Override
  public int[] toIntArray() {
    return data.getInts(offset, length);
  }

  @Override
  public long[] toLongArray() {
    return data.getLongs(offset, length);
  }

  @Override
  public float[] toFloatArray() {
    return data.getFloats(offset, length);
  }

  @Override
  public double[] toDoubleArray() {
    return data.getDoubles(offset, length);
  }

  // TODO: this is extremely expensive.
  @Override
  public Object[] array() {
    DataType dt = data.dataType();
    Object[] list = new Object[length];
    try {
      for (int i = 0; i < length; i++) {
        if (!data.isNullAt(offset + i)) {
          list[i] = get(i, dt);
        }
      }
      return list;
    } catch (Exception e) {
      throw new RuntimeException("Could not get the array", e);
    }
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return data.isNullAt(offset + ordinal);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return data.getBoolean(offset + ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    return data.getByte(offset + ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    return data.getShort(offset + ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    return data.getInt(offset + ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    return data.getLong(offset + ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    return data.getFloat(offset + ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    return data.getDouble(offset + ordinal);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return data.getDecimal(offset + ordinal, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return data.getUTF8String(offset + ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return data.getBinary(offset + ordinal);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return data.getInterval(offset + ordinal);
  }

  @Override
  public TiColumnarRow getStruct(int ordinal, int numFields) {
    return data.getStruct(offset + ordinal);
  }

  @Override
  public TiColumnarArray getArray(int ordinal) {
    return data.getArray(offset + ordinal);
  }

  @Override
  public TiColumnarMap getMap(int ordinal) {
    return data.getMap(offset + ordinal);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType t = (DecimalType) dataType;
      return getDecimal(ordinal, t.precision(), t.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType) dataType).fields().length);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else if (dataType instanceof CalendarIntervalType) {
      return getInterval(ordinal);
    } else {
      throw new UnsupportedOperationException("Datatype not supported " + dataType);
    }
  }

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNullAt(int ordinal) {
    throw new UnsupportedOperationException();
  }
}
