/*
 * Copyright 2019 PingCAP, Inc.
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
 */

package com.pingcap.tikv.catalog;

import com.pingcap.tikv.PDMockServerTest;

public class CatalogTest extends PDMockServerTest {
  //  KVMockServer kvServer;
  //
  //  @Before
  //  @Override
  //  public void setUp() throws IOException {
  //    super.setUp();
  //    kvServer = new KVMockServer();
  //    List<Metapb.Store> s =
  //        ImmutableList.of(
  //            Metapb.Store.newBuilder()
  //                .setAddress("localhost:1234")
  //                .setVersion("5.0.0")
  //                .setId(13)
  //                .build());
  //    kvServer.start(
  //        new TiRegion(
  //            clientSession.getTikvSession().getConf(),
  //            MetaMockHelper.region,
  //            MetaMockHelper.region.getPeers(0),
  //            MetaMockHelper.region.getPeersList(),
  //            s.stream().map(TiStore::new).collect(Collectors.toList())));
  //  }
  //
  //  @Test
  //  public void listDatabasesTest() {
  //    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
  //    helper.preparePDForRegionRead();
  //    helper.setSchemaVersion(666);
  //
  //    helper.addDatabase(130, "global_temp");
  //    helper.addDatabase(264, "TPCH_001");
  //
  //    Catalog cat = clientSession.getCatalog();
  //    List<TiDBInfo> dbs = cat.listDatabases();
  //    List<String> names =
  // dbs.stream().map(TiDBInfo::getName).sorted().collect(Collectors.toList());
  //    assertEquals(2, dbs.size());
  //    assertEquals("global_temp", names.get(0));
  //    assertEquals("tpch_001", names.get(1));
  //
  //    helper.addDatabase(265, "other");
  //    helper.setSchemaVersion(667);
  //
  //    ReflectionWrapper wrapper = new ReflectionWrapper(cat);
  //    wrapper.call("reloadCache");
  //
  //    dbs = cat.listDatabases();
  //    assertEquals(3, dbs.size());
  //    names = dbs.stream().map(TiDBInfo::getName).sorted().collect(Collectors.toList());
  //    assertEquals("global_temp", names.get(0));
  //    assertEquals("other", names.get(1));
  //    assertEquals("tpch_001", names.get(2));
  //
  //    assertEquals(130, cat.getDatabase("global_temp").getId());
  //    assertNull(cat.getDatabase("global_temp111"));
  //  }
  //
  //  @Test
  //  public void listTablesTest() {
  //    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
  //    helper.preparePDForRegionRead();
  //    helper.setSchemaVersion(666);
  //
  //    helper.addDatabase(130, "global_temp");
  //    helper.addDatabase(264, "tpch_001");
  //
  //    helper.addTable(130, 42, "test");
  //    helper.addTable(130, 43, "test1");
  //
  //    Catalog cat = clientSession.getCatalog();
  //    TiDBInfo db = cat.getDatabase("gLObal_temp");
  //    List<TiTableInfo> tables = cat.listTables(db);
  //    List<String> names =
  //        tables.stream().map(TiTableInfo::getName).sorted().collect(Collectors.toList());
  //    assertEquals(2, tables.size());
  //    assertEquals("test", names.get(0));
  //    assertEquals("test1", names.get(1));
  //
  //    assertEquals("test", cat.getTable(db, 42).getName());
  //    assertEquals("test1", cat.getTable(db, 43).getName());
  //    assertNull(cat.getTable(db, 44));
  //
  //    helper.addTable(130, 44, "other");
  //    helper.setSchemaVersion(667);
  //
  //    ReflectionWrapper wrapper = new ReflectionWrapper(cat, boolean.class);
  //    wrapper.call("reloadCache", true);
  //
  //    tables = cat.listTables(db);
  //    names = tables.stream().map(TiTableInfo::getName).sorted().collect(Collectors.toList());
  //    assertEquals(3, tables.size());
  //    assertEquals("other", names.get(0));
  //    assertEquals("test", names.get(1));
  //    assertEquals("test1", names.get(2));
  //
  //    assertEquals(42, cat.getTable("global_temp", "test").getId());
  //    assertNull(cat.getTable("global_temp", "test111"));
  //
  //    helper.dropTable(db.getId(), tables.get(0).getId());
  //    helper.setSchemaVersion(668);
  //    wrapper.call("reloadCache", true);
  //    tables = cat.listTables(db);
  //    assertEquals(2, tables.size());
  //
  //    db = cat.getDatabase("TpCH_001");
  //    tables = cat.listTables(db);
  //    assertTrue(tables.isEmpty());
  //  }
}
