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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.meta;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class DBInfoTest {
  @Test
  public void testSerialize() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json =
        "{\"id\":1,\"db_name\":{\"O\":\"test\",\"L\":\"test\"},\"charset\":\"utf8\",\"collate\":\"utf8_bin\",\"state\":5}";
    TiDBInfo dbInfo = mapper.readValue(json, TiDBInfo.class);
    assertEquals(dbInfo.getId(), 1);
    assertEquals(dbInfo.getName(), "test");
    assertEquals(dbInfo.getCharset(), "utf8");
    assertEquals(dbInfo.getCollate(), "utf8_bin");
    assertEquals(dbInfo.getSchemaState(), SchemaState.StatePublic);
  }
}
