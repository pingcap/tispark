package com.pingcap.tikv.codec;

import static org.junit.Assert.*;

import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.TimestampType;
import gnu.trove.list.array.TLongArrayList;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TableCodecTest {
  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", IntegerType.BIGINT)
        .addColumn("c3", DateTimeType.DATETIME)
        .addColumn("c4", TimestampType.TIMESTAMP)
        .addColumn("c5", StringType.VARCHAR)
        .addColumn("c6", StringType.VARCHAR)
        //        .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  private Object[] values;
  private TiTableInfo tblInfo = createTable();
  private TLongArrayList colIds;

  private void makeValues() {
    List<Object> values = new ArrayList<>();
    values.add(1L);
    values.add(1L);
    DateTime dateTime = DateTime.parse("1995-10-10");
    values.add(new Timestamp(dateTime.getMillis()));
    values.add(new Timestamp(dateTime.getMillis()));
    values.add("abc");
    values.add("中");
    this.values = values.toArray();
  }

  private void makeColIds() {
    TLongArrayList colIds = new TLongArrayList();
    colIds.add(10L);
    colIds.add(11L);
    colIds.add(12L);
    colIds.add(13L);
    colIds.add(14L);
    colIds.add(15L);
    this.colIds = colIds;
  }

  @Before
  public void setUp() {
    makeColIds();
    makeValues();
  }

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testRowCodecThrowException() {
    TLongArrayList fakeColIds = TLongArrayList.wrap(new long[] {1, 2});
    try {
      TableCodec.encodeRow(tblInfo.getColumns(), fakeColIds, values, tblInfo.isPkHandle());
      expectedEx.expect(IllegalAccessException.class);
      expectedEx.expectMessage("encodeRow error: data and columnID count not match 6 vs 2");
    } catch (IllegalAccessException ignored) {
    }
  }

  @Test
  public void testEmptyValues() {
    try {
      byte[] bytes =
          TableCodec.encodeRow(
              new ArrayList<>(), TLongArrayList.wrap(new long[] {}), new Object[] {}, false);
      assertEquals(1, bytes.length);
      assertEquals(Codec.NULL_FLAG, bytes[0]);
    } catch (IllegalAccessException ignored) {
    }
  }

  @Test
  public void testRowCodec() {
    // multiple test was added since encodeRow refuse its cdo
    for (int i = 0; i < 4; i++) {
      try {
        byte[] bytes =
            TableCodec.encodeRow(tblInfo.getColumns(), colIds, values, tblInfo.isPkHandle());
        // testing the correctness via decodeRow
        Object[] res = TableCodec.decodeRow(new CodecDataInput(bytes), tblInfo.getColumns());
        for (int j = 0; j < tblInfo.getColumns().size(); j++) {
          assertEquals(res[2 * j], colIds.get(j));
          assertEquals(res[2 * j + 1], values[j]);
        }
      } catch (IllegalAccessException ignored) {
      }
    }
  }
}
