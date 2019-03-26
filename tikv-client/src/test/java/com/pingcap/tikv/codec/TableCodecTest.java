package com.pingcap.tikv.codec;

import static org.junit.Assert.*;

import com.pingcap.tikv.types.DataType;
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
  private Object[] values;
  private DataType[] colsType;
  private TLongArrayList colIds;

  private void makeValues() {
    List<Object> values = new ArrayList<>();
    values.add(1L);
    values.add(1L);
    DateTime dateTime = DateTime.parse("1995-10-10");
    values.add(new Timestamp(dateTime.getMillis()));
    values.add(new Timestamp(dateTime.getMillis()));
    values.add("abc");
//    values.add("中");
    this.values = values.toArray();
  }

  private void makeRowTypes() {
    List<DataType> dateTypes = new ArrayList<>();
    dateTypes.add(IntegerType.INT);
    dateTypes.add(IntegerType.BIGINT);
    dateTypes.add(DateTimeType.DATETIME);
    dateTypes.add(TimestampType.TIMESTAMP);
    dateTypes.add(StringType.VARCHAR);
//    dateTypes.add(StringType.VARCHAR);
    this.colsType = dateTypes.toArray(new DataType[0]);
  }

  private void makeColIds() {
    TLongArrayList colIds = new TLongArrayList();
    colIds.add(10L);
    colIds.add(11L);
    colIds.add(12L);
    colIds.add(13L);
    colIds.add(14L);
//    colIds.add(15L);
    this.colIds = colIds;
  }

  @Before
  public void setUp() {
    makeColIds();
    makeRowTypes();
    makeValues();
  }

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testRowCodecThrowException() {
    TLongArrayList fakeColIds = TLongArrayList.wrap(new long[] {1, 2});
    try {
      TableCodec.encodeRow(colsType, fakeColIds, values, new CodecDataOutput());
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
              new DataType[] {},
              TLongArrayList.wrap(new long[] {}),
              new Object[] {},
              new CodecDataOutput());
      assertEquals(1, bytes.length);
      assertEquals(Codec.NULL_FLAG, bytes[0]);
    } catch (IllegalAccessException ignored) {
    }
  }

  @Test
  public void testRowCodec() {
    try {
      byte[] bytes = TableCodec.encodeRow(colsType, colIds, values, new CodecDataOutput());
      // testing the correctness via decodeRow
      Object[] res = TableCodec.decodeRow(new CodecDataInput(bytes), colsType);
      for (int i = 0; i < colsType.length; i++) {
        assertEquals(res[2 * i], colIds.get(i));
        assertEquals(res[2 * i + 1], values[i]);
      }
    } catch (IllegalAccessException ignored) {
    }
  }
}
