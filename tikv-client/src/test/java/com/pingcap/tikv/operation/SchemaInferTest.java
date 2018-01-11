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

package com.pingcap.tikv.operation;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.catalog.CatalogTransaction;
import com.pingcap.tikv.expression.*;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import org.junit.Test;

import java.util.List;

import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.plus;
import static com.pingcap.tikv.expression.visitor.ExpressionTypeCoercer.inferType;
import static org.junit.Assert.assertEquals;

public class SchemaInferTest {
  private final String table29 =
      "{\"id\":29,\"name\":{\"O\":\"t1\",\"L\":\"t1\"},\"charset\":\"\",\"collate\":\"\",\"cols\":[{\"id\":1,\"name\":{\"O\":\"time\",\"L\":\"time\"},\"offset\":0,\"origin_default\":null,\"default\":null,\"type\":{\"Tp\":10,\"Flag\":128,\"Flen\":-1,\"Decimal\":-1,\"Charset\":\"binary\",\"Collate\":\"binary\",\"Elems\":null},\"state\":5,\"comment\":\"\"},{\"id\":2,\"name\":{\"O\":\"number\",\"L\":\"number\"},\"offset\":1,\"origin_default\":null,\"default\":null,\"type\":{\"Tp\":3,\"Flag\":128,\"Flen\":-1,\"Decimal\":-1,\"Charset\":\"binary\",\"Collate\":\"binary\",\"Elems\":null},\"state\":5,\"comment\":\"\"},{\"id\":3,\"name\":{\"O\":\"name\",\"L\":\"name\"},\"offset\":2,\"origin_default\":null,\"default\":null,\"type\":{\"Tp\":15,\"Flag\":0,\"Flen\":-1,\"Decimal\":-1,\"Charset\":\"utf8\",\"Collate\":\"utf8_bin\",\"Elems\":null},\"state\":5,\"comment\":\"\"}],\"index_info\":null,\"fk_info\":null,\"state\":5,\"pk_is_handle\":false,\"comment\":\"\",\"auto_inc_id\":0,\"max_col_id\":3,\"max_idx_id\":0}";
  private final ByteString table29Bs = ByteString.copyFromUtf8(table29);

  private TiTableInfo table = CatalogTransaction.parseFromJson(table29Bs, TiTableInfo.class);
  private Expression number = ColumnRef.create("number", table);
  private ColumnRef name = ColumnRef.create("name", table);
  private Expression sum = FunctionCall.newCall("sum", number);
  private ByItem simpleGroupBy = ByItem.create(name, false);
  private ByItem complexGroupBy = ByItem.create(plus(name, Constant.create("1", StringType.VARCHAR)), false);

  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("name", IntegerType.INT, true)
        .build();
  }

  @Test
  public void simpleSelectSchemaInferTest() throws Exception {
    // select name from t1;
    TiDAGRequest tiDAGRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    tiDAGRequest.getFields().add(name);
    List<DataType> dataTypes = SchemaInfer.create(tiDAGRequest).getTypes();
    assertEquals(1, dataTypes.size());
    assertEquals(StringType.VARCHAR.getClass(), dataTypes.get(0).getClass());
  }

  @Test
  public void selectAggSchemaInferTest() throws Exception {
    // select sum(number) from t1;
    TiDAGRequest tiDAGRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    tiDAGRequest.addAggregate(sum, inferType(sum));
    List<DataType> dataTypes = SchemaInfer.create(tiDAGRequest).getTypes();
    assertEquals(1, dataTypes.size());
    assertEquals(DecimalType.DECIMAL.getClass(), dataTypes.get(0).getClass());
  }

  @Test
  public void selectAggWithGroupBySchemaInferTest() throws Exception {
    // select sum(number) from t1 group by name;
    TiDAGRequest dagRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    dagRequest.setTableInfo(createTable());
    dagRequest.getFields().add(name);
    dagRequest.addAggregate(sum, inferType(sum));
    dagRequest.getGroupByItems().add(simpleGroupBy);
    dagRequest.resolve();
    List<DataType> dataTypes = SchemaInfer.create(dagRequest).getTypes();
    assertEquals(2, dataTypes.size());
    assertEquals(DecimalType.DECIMAL.getClass(), dataTypes.get(0).getClass());
    assertEquals(StringType.VARCHAR.getClass(), dataTypes.get(1).getClass());
  }

  @Test
  public void complexGroupBySelectTest() throws Exception {
    // select sum(number) from t1 group by name + "1";
    TiDAGRequest dagRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    dagRequest.setTableInfo(createTable());
    dagRequest.getFields().add(name);
    dagRequest.addAggregate(sum, inferType(sum));
    dagRequest.getGroupByItems().add(complexGroupBy);
    dagRequest.resolve();
    List<DataType> dataTypes = SchemaInfer.create(dagRequest).getTypes();
    assertEquals(2, dataTypes.size());
    assertEquals(DecimalType.DECIMAL.getClass(), dataTypes.get(0).getClass());
    assertEquals(StringType.VARCHAR.getClass(), dataTypes.get(1).getClass());
  }
}
