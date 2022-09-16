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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv.types;

import static org.junit.Assert.assertEquals;

import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import org.junit.Test;
import org.tikv.shade.com.google.common.collect.ImmutableList;

public class DataTypeFactoryTest {

  private static InternalTypeHolder createHolder(MySQLType type) {
    return new InternalTypeHolder(type.getTypeCode(), 0, 0, 0, "", "", ImmutableList.of());
  }

  private void mappingTest(MySQLType type, Class<? extends DataType> cls) {
    InternalTypeHolder holder = createHolder(type);
    DataType dataType = DataTypeFactory.of(holder);
    assertEquals(type, dataType.getType());
    assertEquals(cls, dataType.getClass());
  }

  @Test
  public void of() {
    mappingTest(MySQLType.TypeBit, BitType.class);
    mappingTest(MySQLType.TypeLong, IntegerType.class);
    mappingTest(MySQLType.TypeTiny, IntegerType.class);
    mappingTest(MySQLType.TypeVarchar, StringType.class);
    mappingTest(MySQLType.TypeDate, DateType.class);
  }
}
