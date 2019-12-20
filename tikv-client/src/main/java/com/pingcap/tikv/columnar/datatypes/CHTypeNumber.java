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
    public static final CHTypeUInt8 instance = new CHTypeUInt8();

    private CHTypeUInt8() {
      this.length = 0;
    }

    @Override
    public String name() {
      return "UInt8";
    }
  }

  public static class CHTypeUInt16 extends CHTypeNumber {
    public static final CHTypeUInt16 instance = new CHTypeUInt16();

    private CHTypeUInt16() {
      this.length = 1;
    }

    @Override
    public String name() {
      return "UInt16";
    }
  }

  public static class CHTypeUInt32 extends CHTypeNumber {
    public static final CHTypeUInt32 instance = new CHTypeUInt32();

    private CHTypeUInt32() {
      this.length = 2;
    }

    @Override
    public String name() {
      return "UInt32";
    }
  }

  public static class CHTypeUInt64 extends CHTypeNumber {
    public static final CHTypeUInt64 instance = new CHTypeUInt64();

    private CHTypeUInt64() {
      this.length = 3;
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
    public static final CHTypeInt8 instance = new CHTypeInt8();

    private CHTypeInt8() {
      this.length = 0;
    }

    @Override
    public String name() {
      return "Int8";
    }
  }

  public static class CHTypeInt16 extends CHTypeNumber {
    public static final CHTypeInt16 instance = new CHTypeInt16();

    private CHTypeInt16() {
      this.length = 1;
    }

    @Override
    public String name() {
      return "Int16";
    }
  }

  public static class CHTypeInt32 extends CHTypeNumber {
    public static final CHTypeInt32 instance = new CHTypeInt32();

    private CHTypeInt32() {
      this.length = 2;
    }

    @Override
    public String name() {
      return "Int32";
    }
  }

  public static class CHTypeInt64 extends CHTypeNumber {
    public static final CHTypeInt64 instance = new CHTypeInt64();

    private CHTypeInt64() {
      this.length = 3;
    }

    @Override
    public String name() {
      return "Int64";
    }
  }

  public static class CHTypeFloat32 extends CHTypeNumber {
    public static final CHTypeFloat32 instance = new CHTypeFloat32();

    private CHTypeFloat32() {
      this.length = 2;
    }

    @Override
    public String name() {
      return "Float32";
    }
  }

  public static class CHTypeFloat64 extends CHTypeNumber {
    public static final CHTypeFloat64 instance = new CHTypeFloat64();

    private CHTypeFloat64() {
      this.length = 3;
    }

    @Override
    public String name() {
      return "Float64";
    }
  }
}
