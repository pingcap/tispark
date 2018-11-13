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
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import java.io.*;
import org.junit.Test;

public class TiTableInfoTest {
  public static final String tableJson =
      "\n"
          + "{\n"
          + "   \"id\": 42,\n"
          + "   \"name\": {\n"
          + "      \"O\": \"test\",\n"
          + "      \"L\": \"test\"\n"
          + "   },\n"
          + "   \"charset\": \"\",\n"
          + "   \"collate\": \"\",\n"
          + "   \"cols\": [\n"
          + "      {\n"
          + "         \"id\": 1,\n"
          + "         \"name\": {\n"
          + "            \"O\": \"c1\",\n"
          + "            \"L\": \"c1\"\n"
          + "         },\n"
          + "         \"offset\": 0,\n"
          + "         \"origin_default\": null,\n"
          + "         \"default\": null,\n"
          + "         \"type\": {\n"
          + "            \"Tp\": 3,\n"
          + "            \"Flag\": 139,\n"
          + "            \"Flen\": 11,\n"
          + "            \"Decimal\": -1,\n"
          + "            \"Charset\": \"binary\",\n"
          + "            \"Collate\": \"binary\",\n"
          + "            \"Elems\": null\n"
          + "         },\n"
          + "         \"state\": 5,\n"
          + "         \"comment\": \"\"\n"
          + "      },\n"
          + "      {\n"
          + "         \"id\": 2,\n"
          + "         \"name\": {\n"
          + "            \"O\": \"c2\",\n"
          + "            \"L\": \"c2\"\n"
          + "         },\n"
          + "         \"offset\": 1,\n"
          + "         \"origin_default\": null,\n"
          + "         \"default\": null,\n"
          + "         \"type\": {\n"
          + "            \"Tp\": 15,\n"
          + "            \"Flag\": 0,\n"
          + "            \"Flen\": 100,\n"
          + "            \"Decimal\": -1,\n"
          + "            \"Charset\": \"utf8\",\n"
          + "            \"Collate\": \"utf8_bin\",\n"
          + "            \"Elems\": null\n"
          + "         },\n"
          + "         \"state\": 5,\n"
          + "         \"comment\": \"\"\n"
          + "      },\n"
          + "      {\n"
          + "         \"id\": 3,\n"
          + "         \"name\": {\n"
          + "            \"O\": \"c3\",\n"
          + "            \"L\": \"c3\"\n"
          + "         },\n"
          + "         \"offset\": 2,\n"
          + "         \"origin_default\": null,\n"
          + "         \"default\": null,\n"
          + "         \"type\": {\n"
          + "            \"Tp\": 15,\n"
          + "            \"Flag\": 0,\n"
          + "            \"Flen\": 100,\n"
          + "            \"Decimal\": -1,\n"
          + "            \"Charset\": \"utf8\",\n"
          + "            \"Collate\": \"utf8_bin\",\n"
          + "            \"Elems\": null\n"
          + "         },\n"
          + "         \"state\": 5,\n"
          + "         \"comment\": \"\"\n"
          + "      },\n"
          + "      {\n"
          + "         \"id\": 4,\n"
          + "         \"name\": {\n"
          + "            \"O\": \"c4\",\n"
          + "            \"L\": \"c4\"\n"
          + "         },\n"
          + "         \"offset\": 3,\n"
          + "         \"origin_default\": null,\n"
          + "         \"default\": null,\n"
          + "         \"type\": {\n"
          + "            \"Tp\": 3,\n"
          + "            \"Flag\": 128,\n"
          + "            \"Flen\": 11,\n"
          + "            \"Decimal\": -1,\n"
          + "            \"Charset\": \"binary\",\n"
          + "            \"Collate\": \"binary\",\n"
          + "            \"Elems\": null\n"
          + "         },\n"
          + "         \"state\": 5,\n"
          + "         \"comment\": \"\"\n"
          + "      }\n"
          + "   ],\n"
          + "   \"index_info\": [\n"
          + "      {\n"
          + "         \"id\": 1,\n"
          + "         \"idx_name\": {\n"
          + "            \"O\": \"test_index\",\n"
          + "            \"L\": \"test_index\"\n"
          + "         },\n"
          + "         \"tbl_name\": {\n"
          + "            \"O\": \"\",\n"
          + "            \"L\": \"\"\n"
          + "         },\n"
          + "         \"idx_cols\": [\n"
          + "            {\n"
          + "               \"name\": {\n"
          + "                  \"O\": \"c1\",\n"
          + "                  \"L\": \"c1\"\n"
          + "               },\n"
          + "               \"offset\": 0,\n"
          + "               \"length\": -1\n"
          + "            },\n"
          + "            {\n"
          + "               \"name\": {\n"
          + "                  \"O\": \"c2\",\n"
          + "                  \"L\": \"c2\"\n"
          + "               },\n"
          + "               \"offset\": 1,\n"
          + "               \"length\": -1\n"
          + "            },\n"
          + "            {\n"
          + "               \"name\": {\n"
          + "                  \"O\": \"c3\",\n"
          + "                  \"L\": \"c3\"\n"
          + "               },\n"
          + "               \"offset\": 2,\n"
          + "               \"length\": -1\n"
          + "            }\n"
          + "         ],\n"
          + "         \"is_unique\": false,\n"
          + "         \"is_primary\": false,\n"
          + "         \"state\": 5,\n"
          + "         \"comment\": \"\",\n"
          + "         \"index_type\": 0\n"
          + "      }\n"
          + "   ],\n"
          + "   \"fk_info\": null,\n"
          + "   \"state\": 5,\n"
          + "   \"pk_is_handle\": true,\n"
          + "   \"comment\": \"\",\n"
          + "   \"auto_inc_id\": 0,\n"
          + "   \"max_col_id\": 4,\n"
          + "   \"max_idx_id\": 1\n"
          + "}";

  @Test
  public void testFromJson() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TiTableInfo tableInfo = mapper.readValue(tableJson, TiTableInfo.class);
    assertEquals("test", tableInfo.getName());
    assertEquals(4, tableInfo.getColumns().size());
    assertEquals("c1", tableInfo.getColumns().get(0).getName());
    assertEquals(IntegerType.class, tableInfo.getColumns().get(0).getType().getClass());
    assertEquals("c2", tableInfo.getColumns().get(1).getName());
    assertEquals(StringType.class, tableInfo.getColumns().get(1).getType().getClass());
    assertTrue(tableInfo.isPkHandle());
  }

  @Test
  public void testPartitionInfo() throws Exception {}

  @Test
  public void testPartitionDef() throws Exception {}

  @Test
  public void testSerializable() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TiTableInfo tableInfo = mapper.readValue(tableJson, TiTableInfo.class);
    ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(byteOutStream);
    oos.writeObject(tableInfo);

    ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(byteInStream);
    TiTableInfo deserTable = (TiTableInfo) ois.readObject();
    assertEquals(deserTable.toProto(), tableInfo.toProto());
  }
}
