package com.pingcap.tikv.codec;

import static org.junit.Assert.*;

import com.pingcap.tikv.row.DefaultRowReader;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
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
import org.junit.Test;

public class TableCodecTest {
  private Object[] values;
  private DataType[] rows;
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

  private void makeRowTypes() {
    List<DataType> dateTypes = new ArrayList<>();
    dateTypes.add(IntegerType.INT);
    dateTypes.add(IntegerType.BIGINT);
    dateTypes.add(DateTimeType.DATETIME);
    dateTypes.add(TimestampType.TIMESTAMP);
    dateTypes.add(StringType.VARCHAR);
    dateTypes.add(StringType.VARCHAR);
    this.rows = dateTypes.toArray(new DataType[0]);
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

  @Test
  public void testRowCodec() {
    // Make sure empty row return not nil value.
    makeColIds();
    makeRowTypes();
    makeValues();

    try {
      byte[] bytes = TableCodec.encodeRow(rows, colIds, values, new CodecDataOutput());
      RowReader rowReader = DefaultRowReader.create(new CodecDataInput(bytes));
      List<DataType> newRows = new ArrayList<>();
      for (int i = 0; i < rows.length; i++) {
        newRows.add(IntegerType.BIGINT);
        newRows.add(rows[i]);
      }
      Row row = rowReader.readRow(newRows.toArray(new DataType[0]));
      for (int i = 0; i < rows.length; i++) {
        assertEquals(row.get(2 * i, IntegerType.BIGINT), colIds.get(i));
        assertEquals(row.get(2 * i + 1, rows[i]), values[i]);
      }
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}
