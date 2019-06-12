package com.pingcap.tikv.codec;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.MySQLType;
import com.pingcap.tikv.types.StringType;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TableCodecTest {
  private static TiTableInfo createTable() {
    StringType VARCHAR255 =
        new StringType(
            new TiColumnInfo.InternalTypeHolder(
                MySQLType.TypeVarchar.getTypeCode(), 0, 255, 0, "", "", ImmutableList.of()));

    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", IntegerType.BIGINT)
        // TODO: enable when support Timestamp
        // .addColumn("c3", DateTimeType.DATETIME)
        // .addColumn("c4", TimestampType.TIMESTAMP)
        .addColumn("c5", VARCHAR255)
        .addColumn("c6", VARCHAR255)
        // .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  private Object[] values;
  private TiTableInfo tblInfo = createTable();

  private void makeValues() {
    List<Object> values = new ArrayList<>();
    values.add(1L);
    values.add(1L);
    DateTime dateTime = DateTime.parse("1995-10-10");
    // values.add(new Timestamp(dateTime.getMillis()));
    // values.add(new Timestamp(dateTime.getMillis()));
    values.add("abc");
    values.add("中");
    this.values = values.toArray();
  }

  @Before
  public void setUp() {
    makeValues();
  }

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testRowCodecThrowException() {
    try {
      TableCodec.encodeRow(
          tblInfo.getColumns(), new Object[] {values[0], values[1]}, tblInfo.isPkHandle());
      expectedEx.expect(IllegalAccessException.class);
      expectedEx.expectMessage("encodeRow error: data and columnID count not match 6 vs 2");
    } catch (IllegalAccessException ignored) {
    }
  }

  @Test
  public void testEmptyValues() {
    try {
      byte[] bytes = TableCodec.encodeRow(new ArrayList<>(), new Object[] {}, false);
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
        byte[] bytes = TableCodec.encodeRow(tblInfo.getColumns(), values, tblInfo.isPkHandle());
        // testing the correctness via decodeRow
        Row row = TableCodec.decodeRow(bytes, -1, tblInfo);
        for (int j = 0; j < tblInfo.getColumns().size(); j++) {
          assertEquals(row.get(j, null), values[j]);
        }
      } catch (IllegalAccessException ignored) {
      }
    }
  }
}
