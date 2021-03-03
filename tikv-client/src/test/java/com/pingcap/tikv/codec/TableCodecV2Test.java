/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.codec;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.key.IntHandle;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.MySQLType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.TimeType;
import com.pingcap.tikv.types.TimestampType;
import com.pingcap.tikv.types.UninitializedType;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import org.junit.Test;

public class TableCodecV2Test {

  private static final DataType TEST_BIT_TYPE =
      DataTypeFactory.of(
          new TiColumnInfo.InternalTypeHolder(
              MySQLType.TypeBit.getTypeCode(), 0, 24, 0, "", "", ImmutableList.of()));
  private static final DataType TEST_ENUM_TYPE =
      DataTypeFactory.of(
          new TiColumnInfo.InternalTypeHolder(
              MySQLType.TypeEnum.getTypeCode(), 0, 24, 0, "", "", ImmutableList.of("y", "n")));
  private static final DataType TEST_SET_TYPE =
      DataTypeFactory.of(
          new TiColumnInfo.InternalTypeHolder(
              MySQLType.TypeSet.getTypeCode(), 0, 24, 0, "", "", ImmutableList.of("n1", "n2")));

  @Test
  public void testLargeColID() {
    TestCase testCase =
        TestCase.createNew(
            new int[] {128, 1, 1, 0, 0, 0, 44, 1, 0, 0, 0, 0, 0, 0},
            MetaUtils.TableBuilder.newBuilder()
                .name("t")
                .addColumn("c1", StringType.CHAR, 300)
                .build(),
            new IntHandle(300L),
            new Object[] {""});
    testCase.test();
  }

  @Test
  public void testColumnDataType() {
    long timezoneOffset = TimeZone.getDefault().getRawOffset();
    List<TestCase> testCases =
        ImmutableList.of(
            // test int
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 1, 0, 2},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", IntegerType.INT)
                    .build(),
                new Object[] {2L}),
            // test int64
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 208, 237, 144, 46, 0, 0, 0},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", IntegerType.BIGINT)
                    .build(),
                new Object[] {200000000000L}),
            // test float
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 192, 39, 250, 225, 64, 0, 0, 0},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", RealType.FLOAT)
                    .build(),
                new Object[] {11.99f}),
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 63, 216, 5, 30, 191, 255, 255, 255},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", RealType.FLOAT)
                    .build(),
                new Object[] {-11.99f}),
            // test double
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 192, 39, 250, 225, 71, 174, 20, 123},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", RealType.DOUBLE)
                    .build(),
                new Object[] {11.99}),
            // test decimal
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 5, 0, 6, 4, 139, 38, 172},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", DecimalType.DECIMAL)
                    .build(),
                new Object[] {new BigDecimal("11.9900")}),
            // test bit
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 4, 0, 48, 48, 49, 0},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", TEST_BIT_TYPE, false)
                    .build(),
                new Object[] {new byte[] {49, 48, 48}}),
            // test date
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 69, 77, 105, 166, 25},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", DateType.DATE)
                    .build(),
                new Object[] {new Date(1590007985000L - timezoneOffset)}),
            // test datetime
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 69, 77, 105, 166, 25},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", DateTimeType.DATETIME)
                    .build(),
                new Object[] {new Timestamp(1590007985000L - timezoneOffset)}),
            // test timestamp
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 69, 77, 105, 166, 25},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", TimestampType.TIMESTAMP)
                    .build(),
                new Object[] {new Timestamp(1590007985000L)}),
            // test duration
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 8, 0, 0, 198, 34, 193, 197, 13, 0, 0},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", TimeType.TIME)
                    .build(),
                new Object[] {15143000000000L}),
            // test year
            TestCase.createNew(
                new int[] {128, 0, 1, 0, 0, 0, 1, 2, 0, 228, 7},
                MetaUtils.TableBuilder.newBuilder()
                    .name("t")
                    .addColumn("c1", IntegerType.YEAR)
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
                    .addColumn("c1", StringType.VARCHAR)
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
                    .addColumn("c1", BytesType.LONG_TEXT)
                    .build(),
                new Object[] {
                  new byte[] {
                    0x74, 0x65, 0x73, 0x74, 0x20, 0x60, 0x73, 0x27, 0x74, 0x22, 0x72, 0x2e
                  }
                }));
    // Test Decoding
    for (TestCase testCase : testCases) {
      testCase.test();
    }
  }

  @Test
  public void testHandle() {
    TestCase testCase =
        TestCase.createNew(
            new int[] {128, 0, 1, 0, 0, 0, 10, 1, 0, 1},
            MetaUtils.TableBuilder.newBuilder()
                .name("t")
                .addColumn("pk", IntegerType.BIGINT, true, -1)
                .addColumn("c1", IntegerType.BIGINT, false, 10)
                .setPkHandle(true)
                .build(),
            new IntHandle(10000L),
            new Object[] {new IntHandle(10000L), 1L});
    testCase.test();
  }

  @Test
  public void testNullValues() {
    TestCase testCase =
        TestCase.createNew(
            new int[] {128, 0, 1, 0, 1, 0, 2, 1, 1, 0, 2},
            MetaUtils.TableBuilder.newBuilder()
                .name("t")
                .addColumn("c1", IntegerType.BIGINT)
                .addColumn("c2", IntegerType.BIGINT)
                .build(),
            new Object[] {null, 2L});
    testCase.test();
  }

  @Test
  public void testNewRowTypes() {
    // TODO: Enable JSON and Set type encoding
    MetaUtils.TableBuilder tableBuilder =
        MetaUtils.TableBuilder.newBuilder()
            .name("t")
            .addColumn("c1", IntegerType.BIGINT, 1)
            .addColumn("c2", IntegerType.SMALLINT, 22)
            .addColumn("c3", RealType.DOUBLE, 3)
            .addColumn("c4", BytesType.BLOB, 24)
            .addColumn("c5", StringType.CHAR, 25)
            .addColumn("c6", TimestampType.TIMESTAMP, 5)
            .addColumn("c7", TimeType.TIME, 16)
            .addColumn("c8", DecimalType.DECIMAL, 8)
            .addColumn("c9", IntegerType.YEAR, 12)
            .addColumn("c10", TEST_ENUM_TYPE, 9)
            // .addColumn("c11", JsonType.JSON, 14)
            .addColumn("c12", UninitializedType.NULL, 11) // null
            .addColumn("c13", UninitializedType.NULL, 2) // null
            .addColumn("c14", UninitializedType.NULL, 100) // null
            .addColumn("c15", RealType.FLOAT, 116)
            // .addColumn("c16", TEST_SET_TYPE, 117)
            .addColumn("c17", TEST_BIT_TYPE, 118)
            .addColumn("c18", StringType.VAR_STRING, 119);
    TiTableInfo tbl = tableBuilder.build();
    Timestamp ts = new Timestamp(1320923471000L);
    ts.setNanos(999999000);
    TestCase testCase =
        TestCase.createNew(
            // new int[] {
            //   128, 0, 15, 0, 3, 0, 1, 3, 5, 8, 9, 12, 14, 16, 22, 24, 25, 116, 117, 118, 119, 2,
            // 11,
            //   100, 1, 0, 9, 0, 17, 0, 22, 0, 23, 0, 25, 0, 54, 0, 62, 0, 63, 0, 66, 0, 68, 0, 76,
            // 0,
            //   77, 0, 81, 0, 81, 0, 1, 192, 0, 0, 0, 0, 0, 0, 0, 63, 66, 15, 203, 178, 148, 138,
            // 25,
            //   6, 4, 139, 38, 172, 2, 207, 7, 1, 1, 0, 0, 0, 28, 0, 0, 0, 19, 0, 0, 0, 1, 0, 9,
            // 20,
            //   0, 0, 0, 97, 2, 0, 0, 0, 0, 0, 0, 0, 0, 128, 226, 194, 24, 13, 0, 0, 1, 97, 98, 99,
            //   97, 98, 192, 24, 0, 0, 0, 0, 0, 0, 1, 48, 48, 49, 0
            // },
            new int[] {
              128, 0, 13, 0, 3, 0, 1, 3, 5, 8, 9, 12, 16, 22, 24, 25, 116, 118, 119, 2, 11, 100, 1,
              0, 9, 0, 17, 0, 22, 0, 23, 0, 25, 0, 33, 0, 34, 0, 37, 0, 39, 0, 47, 0, 51, 0, 51, 0,
              1, 192, 0, 0, 0, 0, 0, 0, 0, 63, 66, 15, 203, 178, 148, 138, 25, 6, 4, 139, 38, 172,
              2, 207, 7, 0, 128, 226, 194, 24, 13, 0, 0, 1, 97, 98, 99, 97, 98, 192, 24, 0, 0, 0, 0,
              0, 0, 48, 48, 49, 0
            },
            tbl,
            new Object[] {
              1L,
              1L,
              2.0,
              "abc".getBytes(),
              "ab",
              ts,
              4 * 3600 * 1000000000L,
              new BigDecimal("11.9900"),
              1999L,
              "n",
              // "{\"a\":2}",
              null,
              null,
              null,
              6.0,
              // "n1",
              new byte[] {49, 48, 48},
              ""
            });
    testCase.test();
  }

  public static class TestCase {
    private final byte[] bytes;
    private final TiTableInfo tableInfo;
    private final Handle handle;
    private final Object[] value;

    private TestCase(int[] unsignedBytes, TiTableInfo tableInfo, Handle handle, Object[] value) {
      this.bytes = new byte[unsignedBytes.length];
      for (int i = 0; i < unsignedBytes.length; i++) {
        this.bytes[i] = (byte) (unsignedBytes[i] & 0xff);
      }
      this.tableInfo = tableInfo;
      this.handle = handle;
      this.value = value;
    }

    public static TestCase createNew(
        int[] unsignedBytes, TiTableInfo tableInfo, Handle handle, Object[] value) {
      return new TestCase(unsignedBytes, tableInfo, handle, value);
    }

    public static TestCase createNew(int[] unsignedBytes, TiTableInfo tableInfo, Object[] value) {
      return new TestCase(unsignedBytes, tableInfo, new IntHandle(-1L), value);
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

    private Object[] fromRow(Row row, TiTableInfo tableInfo) {
      Object[] res = new Object[row.fieldCount()];
      for (int i = 0; i < row.fieldCount(); i++) {
        DataType colTp = tableInfo.getColumns().get(i).getType();
        res[i] = row.get(i, colTp);
      }
      return res;
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

    private void debug(byte[] a1, byte[] a2) {
      System.out.println(
          "Expected=[" + Arrays.toString(a1) + "]\nResult  =[" + Arrays.toString(a2) + "]");
    }

    private void testDecode() {
      Row res = TableCodecV2.decodeRow(this.bytes, this.handle, this.tableInfo);
      Object[] o = fromRow(res, this.tableInfo);
      debug(this.value, o);
      assertTrue(deepEquals(this.value, o));
    }

    private void testEncode() {
      byte[] bytes =
          TableCodecV2.encodeRow(
              this.tableInfo.getColumns(), this.value, this.tableInfo.isPkHandle());
      debug(this.bytes, bytes);
      assertTrue(equals(this.bytes, bytes));
    }

    private void test() {
      testDecode();
      testEncode();
    }
  }
}
