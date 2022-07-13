/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.key;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.pingcap.tikv.meta.CIStr;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class IndexKeyTest {

  @Test
  public void createTest() {
    Key k1 = Key.toRawKey(new byte[] {1, 2, 3, 4});
    Key k2 = Key.toRawKey(new byte[] {5, 6, 7, 8});
    Key k3 = Key.toRawKey(new byte[] {5, 6, 7, 9});
    IndexKey ik1 = IndexKey.toIndexKey(666, 777, k1, k2);
    IndexKey ik2 = IndexKey.toIndexKey(666, 777, k1, k2);
    IndexKey ik3 = IndexKey.toIndexKey(666, 777, k1, k3);
    assertEquals(ik1, ik2);
    assertEquals(0, ik1.compareTo(ik2));
    assertTrue(ik1.compareTo(ik3) < 0);
    assertEquals(2, ik1.getDataKeys().length);
    assertEquals(k1, ik1.getDataKeys()[0]);
    assertEquals(k2, ik1.getDataKeys()[1]);

    try {
      IndexKey.toIndexKey(0, 0, k1, null, k2);
      fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void encodeIndexDataValuesTest() {
    TiColumnInfo col1 = new TiColumnInfo(1, "a", 0, IntegerType.INT, false);
    TiColumnInfo col2 = new TiColumnInfo(1, "a", 1, StringType.VARCHAR, true);
    List<TiColumnInfo> tableColumns = new ArrayList<>();
    tableColumns.add(col1);
    tableColumns.add(col2);
    TiTableInfo tableInfo =
        new TiTableInfo(
            1,
            CIStr.newCIStr("test"),
            "",
            "",
            false,
            true,
            tableColumns,
            null,
            "",
            0,
            0,
            0,
            0,
            null,
            null,
            null,
            1,
            1,
            0,
            null,
            0);

    // CommonHandle
    TiIndexColumn index1 = new TiIndexColumn(CIStr.newCIStr("a"), 0, DataType.UNSPECIFIED_LEN);
    List<TiIndexColumn> indexColumns1 = new ArrayList<>();
    indexColumns1.add(index1);
    Object[][] testRows1 =
        new Object[][] {
          new Object[] {null, "1"}, new Object[] {2, "2"},
        };
    Key[][] expectations1 =
        new Key[][] {
          new Key[] {
            TypedKey.toTypedKey(null, IntegerType.INT), TypedKey.toTypedKey("1", StringType.VARCHAR)
          },
          new Key[] {TypedKey.toTypedKey(2, IntegerType.INT)},
        };
    for (int i = 0; i < testRows1.length; i++) {
      Row row = ObjectRowImpl.create(testRows1[i]);
      Handle handle =
          CommonHandle.newCommonHandle(
              new DataType[] {StringType.VARCHAR}, new Object[] {row.get(1, StringType.VARCHAR)});
      IndexKey.EncodeIndexDataResult result =
          IndexKey.encodeIndexDataValues(row, indexColumns1, handle, true, tableInfo);
      assertArrayEquals(expectations1[i], result.keys);
    }

    // PkHandle
    TiIndexColumn index2 = new TiIndexColumn(CIStr.newCIStr("b"), 1, DataType.UNSPECIFIED_LEN);
    List<TiIndexColumn> indexColumns2 = new ArrayList<>();
    Object[][] testRows2 =
        new Object[][] {
          new Object[] {1, null}, new Object[] {2, "2"},
        };
    Key[][] expectations2 =
        new Key[][] {
          new Key[] {
            TypedKey.toTypedKey(null, StringType.VARCHAR),
            TypedKey.toTypedKey(1, IntegerType.BIGINT)
          },
          new Key[] {TypedKey.toTypedKey("2", StringType.VARCHAR)},
        };
    indexColumns2.add(index2);
    for (int i = 0; i < testRows2.length; i++) {
      Row row = ObjectRowImpl.create(testRows2[i]);
      Handle handle = new IntHandle((Integer) row.get(0, IntegerType.BIGINT));
      IndexKey.EncodeIndexDataResult result =
          IndexKey.encodeIndexDataValues(row, indexColumns2, handle, true, tableInfo);
      assertArrayEquals(expectations2[i], result.keys);
    }
  }

  @Test
  public void toStringTest() {
    Key k1 = Key.toRawKey(new byte[] {1, 2, 3, 4});
    TypedKey k2 = TypedKey.toTypedKey(666, IntegerType.INT);
    IndexKey ik = IndexKey.toIndexKey(0, 0, k1, Key.NULL, k2);
    assertArrayEquals(ik.getDataKeys()[0].getBytes(), new byte[] {1, 2, 3, 4});
    assertArrayEquals(ik.getDataKeys()[1].getBytes(), new byte[] {0});
    assertArrayEquals(ik.getDataKeys()[2].getBytes(), new byte[] {3, -128, 0, 0, 0, 0, 0, 2, -102});
  }
}
