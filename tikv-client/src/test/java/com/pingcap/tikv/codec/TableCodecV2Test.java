package com.pingcap.tikv.codec;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.*;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
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

    private static boolean equals(byte[] a, byte[] a2) {
      if (a == a2) return true;
      if (a == null || a2 == null) return false;

      int length = a.length;
      if (a2.length != length) return false;

      for (int i = 0; i < length; i++) if (a[i] != a2[i]) return false;

      return true;
    }

    private static boolean equals(Object a, Object b) {
      if (a == b) return true;
      else if (a == null || b == null) return false;
      else if (a instanceof Double && b instanceof Double)
        return Math.abs(((Double) a) - ((Double) b)) < 1.0e-6;
      else return a.equals(b);
    }

    private static boolean deepEquals0(Object e1, Object e2) {
      assert e1 != null;
      boolean eq;
      if (e1 instanceof Object[] && e2 instanceof Object[])
        eq = deepEquals((Object[]) e1, (Object[]) e2);
      else if (e1 instanceof byte[] && e2 instanceof byte[]) eq = equals((byte[]) e1, (byte[]) e2);
      else eq = equals(e1, e2);
      return eq;
    }

    private void testDecode() {
      Row res = TableCodecV2.decodeRow(this.bytes, this.handle, this.tableInfo);
      System.out.println(
          "Expected="
              + Arrays.toString(this.value)
              + "\nResult  ="
              + Arrays.toString(fromRow(res, this.tableInfo)));
      Object[] o = fromRow(res, this.tableInfo);
      assertEquals(this.value.length, o.length);
      for (int i = 0; i < this.value.length; i++) {
        if (this.value[i] instanceof Double) {
          assertTrue(Math.abs((Double) this.value[i] - ((Number) o[i]).doubleValue()) < 1.0e-6);
        } else {
          assertEquals(this.value[i], o[i]);
        }
      }
    }
  }

  @Test
  public void testLargeColID() {}

  @Test
  public void testDecode() {
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
                new Object[] {new BigDecimal("11.9900")}));

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
  }

  @Test
  public void testEmptyValues() {
    //
  }
}
