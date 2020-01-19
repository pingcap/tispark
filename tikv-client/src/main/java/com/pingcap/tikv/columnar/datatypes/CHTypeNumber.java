package com.pingcap.tikv.columnar.datatypes;

import static com.pingcap.tikv.types.DecimalType.BIG_INT_DECIMAL;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;

public abstract class CHTypeNumber extends CHType {
  @Override
  public DataType toDataType() {
    return IntegerType.BIGINT;
  }

  public static class CHTypeUInt8 extends CHTypeNumber {
    public CHTypeUInt8() {
      this.length = 8;
    }

    @Override
    public String name() {
      return "UInt8";
    }
  }

  public static class CHTypeUInt16 extends CHTypeNumber {
    public CHTypeUInt16() {
      this.length = 8 << 1;
    }

    @Override
    public String name() {
      return "UInt16";
    }
  }

  public static class CHTypeUInt32 extends CHTypeNumber {
    public CHTypeUInt32() {
      this.length = 8 << 2;
    }

    @Override
    public String name() {
      return "UInt32";
    }
  }

  public static class CHTypeUInt64 extends CHTypeNumber {
    public CHTypeUInt64() {
      this.length = 8 << 3;
    }

    @Override
    public String name() {
      return "UInt64";
    }

    @Override
    public DataType toDataType() {
      return BIG_INT_DECIMAL;
    }
  }

  public static class CHTypeInt8 extends CHTypeNumber {
    public CHTypeInt8() {
      this.length = 8;
    }

    @Override
    public String name() {
      return "Int8";
    }
  }

  public static class CHTypeInt16 extends CHTypeNumber {

    public CHTypeInt16() {
      this.length = 8 << 1;
    }

    @Override
    public String name() {
      return "Int16";
    }
  }

  public static class CHTypeInt32 extends CHTypeNumber {
    public CHTypeInt32() {
      this.length = 8 << 2;
    }

    @Override
    public String name() {
      return "Int32";
    }
  }

  public static class CHTypeInt64 extends CHTypeNumber {
    public CHTypeInt64() {
      this.length = 8 << 3;
    }

    @Override
    public String name() {
      return "Int64";
    }
  }

  public static class CHTypeFloat32 extends CHTypeNumber {
    public CHTypeFloat32() {
      this.length = 8 << 2;
    }

    @Override
    public String name() {
      return "Float32";
    }
  }

  public static class CHTypeFloat64 extends CHTypeNumber {
    public CHTypeFloat64() {
      this.length = 8 << 3;
    }

    @Override
    public String name() {
      return "Float64";
    }
  }
}
