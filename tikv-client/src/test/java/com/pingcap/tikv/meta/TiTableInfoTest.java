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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

public class TiTableInfoTest {
  public static final String partitionDef =
      "{\n"
          + "   \"id\": 85,\n"
          + "   \"name\": {\n"
          + "      \"O\": \"p2\",\n"
          + "      \"L\": \"p2\"\n"
          + "   },\n"
          + "   \"less_than\": [\n"
          + "      \"2000\"\n"
          + "   ]\n"
          + "}";

  public static final String partitionInfo =
      "{\n"
          + "      \"type\":1,\n"
          + "      \"expr\":\"year(`purchased`)\",\n"
          + "      \"columns\":null,\n"
          + "      \"enable\":true,\n"
          + "      \"definitions\":[\n"
          + "         {\n"
          + "            \"id\":83,\n"
          + "            \"name\":{\n"
          + "               \"O\":\"p0\",\n"
          + "               \"L\":\"p0\"\n"
          + "            },\n"
          + "            \"less_than\":[\n"
          + "               \"1990\"\n"
          + "            ]\n"
          + "         },\n"
          + "         {\n"
          + "            \"id\":84,\n"
          + "            \"name\":{\n"
          + "               \"O\":\"p1\",\n"
          + "               \"L\":\"p1\"\n"
          + "            },\n"
          + "            \"less_than\":[\n"
          + "               \"1995\"\n"
          + "            ]\n"
          + "         },\n"
          + "         {\n"
          + "            \"id\":85,\n"
          + "            \"name\":{\n"
          + "               \"O\":\"p2\",\n"
          + "               \"L\":\"p2\"\n"
          + "            },\n"
          + "            \"less_than\":[\n"
          + "               \"2000\"\n"
          + "            ]\n"
          + "         },\n"
          + "         {\n"
          + "            \"id\":86,\n"
          + "            \"name\":{\n"
          + "               \"O\":\"p3\",\n"
          + "               \"L\":\"p3\"\n"
          + "            },\n"
          + "            \"less_than\":[\n"
          + "               \"2005\"\n"
          + "            ]\n"
          + "         }\n"
          + "      ]\n"
          + "   }\n"
          + "}";
  public static final String partitionTableJson =
      "{\n"
          + "   \"id\":82,\n"
          + "   \"name\":{\n"
          + "      \"O\":\"trb3\",\n"
          + "      \"L\":\"trb3\"\n"
          + "   },\n"
          + "   \"charset\":\"\",\n"
          + "   \"collate\":\"\",\n"
          + "   \"cols\":[\n"
          + "      {\n"
          + "         \"id\":1,\n"
          + "         \"name\":{\n"
          + "            \"O\":\"id\",\n"
          + "            \"L\":\"id\"\n"
          + "         },\n"
          + "         \"offset\":0,\n"
          + "         \"origin_default\":null,\n"
          + "         \"default\":null,\n"
          + "         \"default_bit\":null,\n"
          + "         \"generated_expr_string\":\"\",\n"
          + "         \"generated_stored\":false,\n"
          + "         \"dependences\":null,\n"
          + "         \"type\":{\n"
          + "            \"Tp\":3,\n"
          + "            \"Flag\":0,\n"
          + "            \"Flen\":11,\n"
          + "            \"Decimal\":0,\n"
          + "            \"Charset\":\"binary\",\n"
          + "            \"Collate\":\"binary\",\n"
          + "            \"Elems\":null\n"
          + "         },\n"
          + "         \"state\":5,\n"
          + "         \"comment\":\"\"\n"
          + "      },\n"
          + "      {\n"
          + "         \"id\":2,\n"
          + "         \"name\":{\n"
          + "            \"O\":\"name\",\n"
          + "            \"L\":\"name\"\n"
          + "         },\n"
          + "         \"offset\":1,\n"
          + "         \"origin_default\":null,\n"
          + "         \"default\":null,\n"
          + "         \"default_bit\":null,\n"
          + "         \"generated_expr_string\":\"\",\n"
          + "         \"generated_stored\":false,\n"
          + "         \"dependences\":null,\n"
          + "         \"type\":{\n"
          + "            \"Tp\":15,\n"
          + "            \"Flag\":0,\n"
          + "            \"Flen\":50,\n"
          + "            \"Decimal\":0,\n"
          + "            \"Charset\":\"utf8\",\n"
          + "            \"Collate\":\"utf8_bin\",\n"
          + "            \"Elems\":null\n"
          + "         },\n"
          + "         \"state\":5,\n"
          + "         \"comment\":\"\"\n"
          + "      },\n"
          + "      {\n"
          + "         \"id\":3,\n"
          + "         \"name\":{\n"
          + "            \"O\":\"purchased\",\n"
          + "            \"L\":\"purchased\"\n"
          + "         },\n"
          + "         \"offset\":2,\n"
          + "         \"origin_default\":null,\n"
          + "         \"default\":null,\n"
          + "         \"default_bit\":null,\n"
          + "         \"generated_expr_string\":\"\",\n"
          + "         \"generated_stored\":false,\n"
          + "         \"dependences\":null,\n"
          + "         \"type\":{\n"
          + "            \"Tp\":10,\n"
          + "            \"Flag\":128,\n"
          + "            \"Flen\":10,\n"
          + "            \"Decimal\":0,\n"
          + "            \"Charset\":\"binary\",\n"
          + "            \"Collate\":\"binary\",\n"
          + "            \"Elems\":null\n"
          + "         },\n"
          + "         \"state\":5,\n"
          + "         \"comment\":\"\"\n"
          + "      }\n"
          + "   ],\n"
          + "   \"index_info\":null,\n"
          + "   \"fk_info\":null,\n"
          + "   \"state\":5,\n"
          + "   \"pk_is_handle\":false,\n"
          + "   \"comment\":\"\",\n"
          + "   \"auto_inc_id\":0,\n"
          + "   \"max_col_id\":3,\n"
          + "   \"max_idx_id\":0,\n"
          + "   \"update_timestamp\":404161490771771397,\n"
          + "   \"ShardRowIDBits\":0,\n"
          + "   \"partition\":{\n"
          + "      \"type\":1,\n"
          + "      \"expr\":\"year(`purchased`)\",\n"
          + "      \"columns\":null,\n"
          + "      \"enable\":true,\n"
          + "      \"definitions\":[\n"
          + "         {\n"
          + "            \"id\":83,\n"
          + "            \"name\":{\n"
          + "               \"O\":\"p0\",\n"
          + "               \"L\":\"p0\"\n"
          + "            },\n"
          + "            \"less_than\":[\n"
          + "               \"1990\"\n"
          + "            ]\n"
          + "         },\n"
          + "         {\n"
          + "            \"id\":84,\n"
          + "            \"name\":{\n"
          + "               \"O\":\"p1\",\n"
          + "               \"L\":\"p1\"\n"
          + "            },\n"
          + "            \"less_than\":[\n"
          + "               \"1995\"\n"
          + "            ]\n"
          + "         },\n"
          + "         {\n"
          + "            \"id\":85,\n"
          + "            \"name\":{\n"
          + "               \"O\":\"p2\",\n"
          + "               \"L\":\"p2\"\n"
          + "            },\n"
          + "            \"less_than\":[\n"
          + "               \"2000\"\n"
          + "            ]\n"
          + "         },\n"
          + "         {\n"
          + "            \"id\":86,\n"
          + "            \"name\":{\n"
          + "               \"O\":\"p3\",\n"
          + "               \"L\":\"p3\"\n"
          + "            },\n"
          + "            \"less_than\":[\n"
          + "               \"2005\"\n"
          + "            ]\n"
          + "         }\n"
          + "      ]\n"
          + "   },\n"
          + "   \"compression\":\"\"\n"
          + "}";
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
  public void testPartitionDefFromJson() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TiPartitionDef pDef = mapper.readValue(partitionDef, TiPartitionDef.class);
    assertEquals("p2", pDef.getName());
    assertEquals(85, pDef.getId());
    assertEquals("2000", pDef.getLessThan().get(0));
  }

  @Test
  public void testPartitionInfoFromJson() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TiPartitionInfo pInfo = mapper.readValue(partitionInfo, TiPartitionInfo.class);
    assertEquals("year(`purchased`)", pInfo.getExpr());
    assertEquals(4, pInfo.getDefs().size());
  }

  @Test
  public void testPartitionTableFromJson() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TiTableInfo tableInfo = mapper.readValue(partitionTableJson, TiTableInfo.class);
    assertEquals("year(`purchased`)", tableInfo.getPartitionInfo().getExpr());
  }

  @Test
  public void testTableFromJson() throws Exception {
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
