package com.pingcap.tikv.columnar;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
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
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/** Row abstraction in {@link TiColumnVector}. */
public final class TiColumnarRow extends InternalRow {
  // The data for this row.
  // E.g. the value of 3rd int field is `data.getChild(3).getInt(rowId)`.
  private final TiColumnVector data;
  private final int rowId;
  private final int numFields;

  public TiColumnarRow(TiColumnVector data, int rowId) {
    assert (data.dataType() instanceof StructType);
    this.data = data;
    this.rowId = rowId;
    this.numFields = ((StructType) data.dataType()).size();
  }

  @Override
  public int numFields() {
    return numFields;
  }

  /** Revisit this. This is expensive. This is currently only used in test paths. */
  @Override
  public InternalRow copy() {
    GenericInternalRow row = new GenericInternalRow(numFields);
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = data.getChild(i).dataType();
        if (dt instanceof BooleanType) {
          row.setBoolean(i, getBoolean(i));
        } else if (dt instanceof ByteType) {
          row.setByte(i, getByte(i));
        } else if (dt instanceof ShortType) {
          row.setShort(i, getShort(i));
        } else if (dt instanceof IntegerType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof LongType) {
          row.setLong(i, getLong(i));
        } else if (dt instanceof FloatType) {
          row.setFloat(i, getFloat(i));
        } else if (dt instanceof DoubleType) {
          row.setDouble(i, getDouble(i));
        } else if (dt instanceof StringType) {
          row.update(i, getUTF8String(i).copy());
        } else if (dt instanceof BinaryType) {
          row.update(i, getBinary(i));
        } else if (dt instanceof DecimalType) {
          DecimalType t = (DecimalType) dt;
          row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (dt instanceof DateType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof TimestampType) {
          row.setLong(i, getLong(i));
        } else {
          throw new RuntimeException("Not implemented. " + dt);
        }
      }
    }
    return row;
  }

  @Override
  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return data.getChild(ordinal).isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return data.getChild(ordinal).getBoolean(rowId);
  }

  @Override
  public byte getByte(int ordinal) {
    return data.getChild(ordinal).getByte(rowId);
  }

  @Override
  public short getShort(int ordinal) {
    return data.getChild(ordinal).getShort(rowId);
  }

  @Override
  public int getInt(int ordinal) {
    return data.getChild(ordinal).getInt(rowId);
  }

  @Override
  public long getLong(int ordinal) {
    return data.getChild(ordinal).getLong(rowId);
  }

  @Override
  public float getFloat(int ordinal) {
    return data.getChild(ordinal).getFloat(rowId);
  }

  @Override
  public double getDouble(int ordinal) {
    return data.getChild(ordinal).getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return data.getChild(ordinal).getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return data.getChild(ordinal).getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return data.getChild(ordinal).getBinary(rowId);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return data.getChild(ordinal).getInterval(rowId);
  }

  @Override
  public TiColumnarRow getStruct(int ordinal, int numFields) {
    return data.getChild(ordinal).getStruct(rowId);
  }

  @Override
  public TiColumnarArray getArray(int ordinal) {
    return data.getChild(ordinal).getArray(rowId);
  }

  @Override
  public TiColumnarMap getMap(int ordinal) {
    return data.getChild(ordinal).getMap(rowId);
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
