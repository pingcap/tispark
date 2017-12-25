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
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.aggregate.Sum;
import com.pingcap.tikv.expression.scalar.Plus;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import org.junit.Test;

import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_NEW_DECIMAL;
import static com.pingcap.tikv.types.Types.TYPE_VARCHAR;
import static org.junit.Assert.assertEquals;

public class SchemaInferTest {
  private final String table29 =
      "{\"id\":29,\"name\":{\"O\":\"t1\",\"L\":\"t1\"},\"charset\":\"\",\"collate\":\"\",\"cols\":[{\"id\":1,\"name\":{\"O\":\"time\",\"L\":\"time\"},\"offset\":0,\"origin_default\":null,\"default\":null,\"type\":{\"Tp\":10,\"Flag\":128,\"Flen\":-1,\"Decimal\":-1,\"Charset\":\"binary\",\"Collate\":\"binary\",\"Elems\":null},\"state\":5,\"comment\":\"\"},{\"id\":2,\"name\":{\"O\":\"number\",\"L\":\"number\"},\"offset\":1,\"origin_default\":null,\"default\":null,\"type\":{\"Tp\":3,\"Flag\":128,\"Flen\":-1,\"Decimal\":-1,\"Charset\":\"binary\",\"Collate\":\"binary\",\"Elems\":null},\"state\":5,\"comment\":\"\"},{\"id\":3,\"name\":{\"O\":\"name\",\"L\":\"name\"},\"offset\":2,\"origin_default\":null,\"default\":null,\"type\":{\"Tp\":15,\"Flag\":0,\"Flen\":-1,\"Decimal\":-1,\"Charset\":\"utf8\",\"Collate\":\"utf8_bin\",\"Elems\":null},\"state\":5,\"comment\":\"\"}],\"index_info\":null,\"fk_info\":null,\"state\":5,\"pk_is_handle\":false,\"comment\":\"\",\"auto_inc_id\":0,\"max_col_id\":3,\"max_idx_id\":0}";
  private final ByteString table29Bs = ByteString.copyFromUtf8(table29);

  private TiTableInfo table = CatalogTransaction.parseFromJson(table29Bs, TiTableInfo.class);
  private TiExpr number = TiColumnRef.create("number", table);
  private TiColumnRef name = TiColumnRef.create("name", table);
  private TiExpr sum = new Sum(number);
  private TiByItem simpleGroupBy = TiByItem.create(name, false);
  private TiByItem complexGroupBy = TiByItem.create(new Plus(name, TiConstant.create("1")), false);

  @Test
  public void simpleSelectSchemaInferTest() throws Exception {
    // select name from t1;
    TiDAGRequest tiDAGRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    tiDAGRequest.getFields().add(name);
    List<DataType> dataTypes = SchemaInfer.create(tiDAGRequest).getTypes();
    assertEquals(1, dataTypes.size());
    assertEquals(DataTypeFactory.of(TYPE_VARCHAR), dataTypes.get(0));
  }

  @Test
  public void selectAggSchemaInferTest() throws Exception {
    // select sum(number) from t1;
    TiDAGRequest tiDAGRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    tiDAGRequest.addAggregate(sum);
    List<DataType> dataTypes = SchemaInfer.create(tiDAGRequest).getTypes();
    assertEquals(1, dataTypes.size());
    assertEquals(DataTypeFactory.of(TYPE_NEW_DECIMAL), dataTypes.get(0));
  }

  @Test
  public void selectAggWithGroupBySchemaInferTest() throws Exception {
    // select sum(number) from t1 group by name;
    TiDAGRequest dagRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    dagRequest.getFields().add(name);
    dagRequest.addAggregate(sum);
    dagRequest.getGroupByItems().add(simpleGroupBy);
    List<DataType> dataTypes = SchemaInfer.create(dagRequest).getTypes();
    assertEquals(2, dataTypes.size());
    assertEquals(DataTypeFactory.of(TYPE_NEW_DECIMAL), dataTypes.get(0));
    assertEquals(DataTypeFactory.of(TYPE_VARCHAR), dataTypes.get(1));
  }

  @Test
  public void complexGroupBySelectTest() throws Exception {
    // select sum(number) from t1 group by name + "1";
    TiDAGRequest dagRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    dagRequest.getFields().add(name);
    dagRequest.addAggregate(sum);
    dagRequest.getGroupByItems().add(complexGroupBy);
    List<DataType> dataTypes = SchemaInfer.create(dagRequest).getTypes();
    assertEquals(2, dataTypes.size());
    assertEquals(DataTypeFactory.of(TYPE_NEW_DECIMAL), dataTypes.get(0));
    assertEquals(DataTypeFactory.of(TYPE_VARCHAR), dataTypes.get(1));
  }
}
