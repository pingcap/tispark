package com.pingcap.tikv.codec;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import org.junit.Test;

public class TableCodecV2Test {

  public static class TestCase {
    private final byte[] bytes;
    private final TiTableInfo tableInfo;
    private final Long handle;
    private final Object[] value;

    public static TestCase createNew(
        int[] unsignedBytes, TiTableInfo tableInfo, Long handle, Object[] value) {
      return new TestCase(unsignedBytes, tableInfo, handle, value);
    }

    public static TestCase createNew(int[] unsignedBytes, TiTableInfo tableInfo, Object[] value) {
      return new TestCase(unsignedBytes, tableInfo, -1L, value);
    }

    private TestCase(int[] unsignedBytes, TiTableInfo tableInfo, Long handle, Object[] value) {
      this.bytes = new byte[unsignedBytes.length];
      for (int i = 0; i < unsignedBytes.length; i++) {
        this.bytes[i] = (byte) (unsignedBytes[i] & 0xff);
      }
      this.tableInfo = tableInfo;
      this.handle = handle;
      this.value = value;
    }

    private Object[] fromRow(Row row, TiTableInfo tableInfo) {
      Object[] res = new Object[row.fieldCount()];
      for (int i = 0; i < row.fieldCount(); i++) {
        DataType colTp = tableInfo.getColumns().get(i).getType();
        res[i] = row.get(i, colTp);
      }
      return res;
    }

    private static boolean equals(byte[] a1, byte[] a2) {
      assert a1 != null && a2 != null;
      int length = a1.length;
      if (a2.length != length) return false;
      for (int i = 0; i < length; i++) if (a1[i] != a2[i]) return false;
      return true;
    }

    private static boolean equals(Object a1, Object a2) {
      assert a1 != null && a2 != null;
      if (a1 instanceof Double)
        return Math.abs(((Double) a1) - ((Number) a2).doubleValue()) < 1.0e-6;
      else return a1.equals(a2);
    }

    private static boolean deepEquals(Object[] e1, Object[] e2) {
      if (e1.length != e2.length) {
        return false;
      }
      for (int i = 0; i < e1.length; i++) {
        if (!deepEquals0(e1[i], e2[i])) {
          return false;
        }
      }
      return true;
    }

    private static boolean deepEquals0(Object e1, Object e2) {
      if (e1 == e2) return true;
      if (e1 == null || e2 == null) return false;
      boolean eq;
      if (e1 instanceof Object[] && e2 instanceof Object[])
        eq = deepEquals((Object[]) e1, (Object[]) e2);
      else if (e1 instanceof byte[] && e2 instanceof byte[]) eq = equals((byte[]) e1, (byte[]) e2);
      else eq = equals(e1, e2);
      return eq;
    }

    private String toString(Object[] a) {
      StringBuilder s = new StringBuilder();
      for (int i = 0; i < a.length; i++) {
        Object o = a[i];
        if (i > 0) {
          s.append(",");
        }
        if (o == null) {
          s.append("null");
        } else if (o instanceof byte[]) {
          s.append(Arrays.toString((byte[]) o));
        } else {
          s.append(o.toString());
        }
      }
      return s.toString();
    }

    private void debug(Object[] a1, Object[] a2) {
      System.out.println("Expected=[" + toString(a1) + "]\nResult  =[" + toString(a2) + "]");
    }

    private void testDecode() {
      Row res = TableCodecV2.decodeRow(this.bytes, this.handle, this.tableInfo);
      Object[] o = fromRow(res, this.tableInfo);
      debug(this.value, o);
      assertTrue(deepEquals(this.value, o));
    }
  }

  @Test
  public void testLargeColID() {
    TestCase testCase =
        TestCase.createNew(
            new int[] {128, 1, 1, 0, 0, 0, 44, 1, 0, 0, 0, 0, 0, 0},
            MetaUtils.TableBuilder.newBuilder()
                .name("t")
                .addColumn("c1", BitType.BIT, false)
                .build(),
            300L,
            new Object[] {null});
    testCase.testDecode();
  }

  @Test
  public void testDecode() {
    long timezoneOffset = TimeZone.getDefault().getRawOffset();
    List<TestCase> testCases =
        ImmutableList.of(
            // test int
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 1, 0, 2},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", IntegerType.INT, false)
                    .build(),
                new Object[] {2L}),
            // test int64
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 208, 237, 144, 46, 0, 0, 0},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", IntegerType.BIGINT, false)
                    .build(),
                new Object[] {200000000000L}),
            // test float
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 192, 39, 250, 225, 64, 0, 0, 0},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", RealType.FLOAT, false)
                    .build(),
                new Object[] {11.99}),
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 63, 216, 5, 30, 191, 255, 255, 255},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", RealType.FLOAT, false)
                    .build(),
                new Object[] {-11.99}),
            // test decimal
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 5, 0, 6, 4, 139, 38, 172},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", DecimalType.DECIMAL, false)
                    .build(),
                new Object[] {new BigDecimal("11.9900")}),
            // test bit
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 4, 0, 48, 48, 49, 0},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn(
                        "c1",
                        DataTypeFactory.of(
                            new TiColumnInfo.InternalTypeHolder(
                                16, 0, 24, 0, "", "", ImmutableList.of())),
                        false)
                    .build(),
                new Object[] {new byte[] {49, 48, 48}}),
            // test date
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 69, 77, 105, 166, 25},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", DateType.DATE, false)
                    .build(),
                new Object[] {new Date(1590007985000L - timezoneOffset)}),
            // test datetime
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 69, 77, 105, 166, 25},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", DateTimeType.DATETIME, false)
                    .build(),
                new Object[] {new Timestamp(1590007985000L - timezoneOffset)}),
            // test timestamp
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 69, 77, 105, 166, 25},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", TimestampType.TIMESTAMP, false)
                    .build(),
                new Object[] {new Timestamp(1590007985000L)}),
            // test duration
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 198, 34, 193, 197, 13, 0, 0},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", TimeType.TIME, false)
                    .build(),
                new Object[] {15143000000000L}),
            // test year
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 2, 0, 228, 7},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", IntegerType.YEAR, false)
                    .build(),
                new Object[] {2020L}),
            // test varchar
            TestCase.createNew(
                new int[] {
                  128, 0, 1, 0, 0, 0, 1, 39, 0, 116, 101, 115, 116, 32, 119, 105, 116, 104, 32, 115,
                  111, 109, 101, 32, 101, 115, 99, 97, 112, 101, 115, 32, 95, 46, 42, 91, 123, 92,
                  10, 39, 34, 1, 228, 184, 173, 230, 150, 135
                },
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", StringType.VARCHAR, false)
                    .build(),
                new Object[] {"test with some escapes _.*[{\\\n'\"\u0001中文"}),
            // test year
            TestCase.createNew(
                new int[] {
                  128, 0, 1, 0, 0, 0, 1, 12, 0, 116, 101, 115, 116, 32, 96, 115, 39, 116, 34, 114,
                  46
                },
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", BytesType.LONG_TEXT, false)
                    .build(),
                new Object[] {
                  new byte[] {
                    0x74, 0x65, 0x73, 0x74, 0x20, 0x60, 0x73, 0x27, 0x74, 0x22, 0x72, 0x2e
                  }
                }));
    // Test Decoding
    for (TestCase testCase : testCases) {
      testCase.testDecode();
    }
  }

  @Test
  public void testHandle() {
    TestCase testCase =
        TestCase.createNew(
            new int[] {128, 1, 1, 0, 0, 0, 44, 1, 0, 0, 0, 0, 0, 0},
            MetaUtils.TableBuilder.newBuilder()
                .name("t")
                .addColumn("c1", BitType.BIT, false)
                .build(),
            300L,
            new Object[] {null});
    testCase.testDecode();
    TestCase testCase2 =
        TestCase.createNew(
            new int[] {128, 0, 1, 0, 0, 0, 10, 1, 0, 1},
            MetaUtils.TableBuilder.newBuilder()
                .name("t")
                .addColumn("pk", IntegerType.BIGINT, true, -1)
                .addColumn("c1", IntegerType.BIGINT, false, 10)
                .setPkHandle(true)
                .build(),
            10000L,
            new Object[] {10000L, 1L});
    testCase2.testDecode();
  }

  @Test
  public void testNullValues() {
    TestCase testCase =
        TestCase.createNew(
            new int[] {128, 0, 1, 0, 1, 0, 2, 1, 1, 0, 2},
            MetaUtils.TableBuilder.newBuilder()
                .name("t")
                .addColumn("c1", IntegerType.BIGINT, false)
                .addColumn("c2", IntegerType.BIGINT, false)
                .build(),
            new Object[] {null, 2L});
    testCase.testDecode();
  }
}
