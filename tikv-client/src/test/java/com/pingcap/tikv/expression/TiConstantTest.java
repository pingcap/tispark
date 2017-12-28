/*
 *
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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv.expression;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.meta.TiTableInfoTest;
import com.pingcap.tikv.types.RealType;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Test;

import java.sql.Timestamp;

import static com.pingcap.tikv.expression.TiConstant.DateWrapper;
import static org.junit.Assert.assertEquals;

public class TiConstantTest {
  @Test
  public void greaterThanTest() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TiTableInfo tableInfo = mapper.readValue(TiTableInfoTest.tableJson, TiTableInfo.class);
    GreaterThan g = new GreaterThan(TiColumnRef.create("c1", tableInfo), TiConstant.create(1.12));
    Expr ge = g.toProto();
    assertEquals(2, ge.getChildrenCount());
    double expected = RealType.readDouble(new CodecDataInput(ge.getChildren(1).getVal()));
    assertEquals(1.12, expected, 0.00001);
  }

  @Test
  public void testEncodeSQLDate() {
    DateWrapper[] wrappers = new DateWrapper[]{
        new DateWrapper(904694400000L),
        new DateWrapper(946684800000L),
        new DateWrapper(1077984000000L),
        new DateWrapper(-1019721600000L),
        new DateWrapper(1488326400000L),
    };

    DateTimeZone timeZone = DateTimeZone.UTC;
    LocalDate[] localDates = new LocalDate[]{
        new LocalDate(904694400000L, timeZone),
        new LocalDate(946684800000L, timeZone),
        new LocalDate(1077984000000L, timeZone),
        new LocalDate(-1019721600000L, timeZone),
        new LocalDate(1488326400000L, timeZone),
    };

    String[] encodeAnswers = new String[]{
        "tp: MysqlTime\nval: \"\\031_\\304\\000\\000\\000\\000\\000\"\n",
        "tp: MysqlTime\nval: \"\\031dB\\000\\000\\000\\000\\000\"\n",
        "tp: MysqlTime\nval: \"\\031q\\270\\000\\000\\000\\000\\000\"\n",
        "tp: MysqlTime\nval: \"\\030\\231\\220\\000\\000\\000\\000\\000\"\n",
        "tp: MysqlTime\nval: \"\\031\\234\\002\\000\\000\\000\\000\\000\"\n",
    };

    String[][] answers = {
        {"1998", "9", "2"},
        {"2000", "1", "1"},
        {"2004", "2", "28"},
        {"1937", "9", "8"},
        {"2017", "3", "1"},
    };
    assertEquals(answers.length, wrappers.length);

    for (int i = 0; i < wrappers.length; i++) {
      assertEquals(answers[i][0], localDates[i].getYear() + "");
      assertEquals(answers[i][1], localDates[i].getMonthOfYear() + "");
      assertEquals(answers[i][2], localDates[i].getDayOfMonth() + "");
      assertEquals(encodeAnswers[i], TiConstant.create(wrappers[i]).toProto().toString());
    }
  }

  @Test
  public void testEncodeTimestamp() {
    TiConstant tsDate = TiConstant.create(new Timestamp(1998, 9, 2, 19, 0, 0, 0));
    assertEquals("tp: MysqlTime\nval: \"1\\177\\0050\\000\\000\\000\\000\"\n",
        tsDate.toProto().toString());
  }
}
