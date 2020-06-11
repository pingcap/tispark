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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.pingcap.tikv.KVMockServer;
import com.pingcap.tikv.PDMockServerTest;
import com.pingcap.tikv.meta.MetaUtils.MetaMockHelper;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.ReflectionWrapper;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class CatalogTest extends PDMockServerTest {
  KVMockServer kvServer;

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    kvServer = new KVMockServer();
    kvServer.start(
        new TiRegion(
            MetaMockHelper.region,
            MetaMockHelper.region.getPeers(0),
            IsolationLevel.RC,
            CommandPri.Low));
  }

  @Test
  public void listDatabasesTest() {
    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.setSchemaVersion(666);

    helper.addDatabase(130, "global_temp");
    helper.addDatabase(264, "TPCH_001");

    Catalog cat = session.getCatalog();
    List<TiDBInfo> dbs = cat.listDatabases();
    List<String> names = dbs.stream().map(TiDBInfo::getName).sorted().collect(Collectors.toList());
    assertEquals(2, dbs.size());
    assertEquals("global_temp", names.get(0));
    assertEquals("tpch_001", names.get(1));

    helper.addDatabase(265, "other");
    helper.setSchemaVersion(667);

    ReflectionWrapper wrapper = new ReflectionWrapper(cat);
    wrapper.call("reloadCache");

    dbs = cat.listDatabases();
    assertEquals(3, dbs.size());
    names = dbs.stream().map(TiDBInfo::getName).sorted().collect(Collectors.toList());
    assertEquals("global_temp", names.get(0));
    assertEquals("other", names.get(1));
    assertEquals("tpch_001", names.get(2));

    assertEquals(130, cat.getDatabase("global_temp").getId());
    assertNull(cat.getDatabase("global_temp111"));
  }

  @Test
  public void listTablesTest() {
    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.setSchemaVersion(666);

    helper.addDatabase(130, "global_temp");
    helper.addDatabase(264, "tpch_001");

    helper.addTable(130, 42, "test");
    helper.addTable(130, 43, "test1");

    Catalog cat = session.getCatalog();
    TiDBInfo db = cat.getDatabase("gLObal_temp");
    List<TiTableInfo> tables = cat.listTables(db);
    List<String> names =
        tables.stream().map(TiTableInfo::getName).sorted().collect(Collectors.toList());
    assertEquals(2, tables.size());
    assertEquals("test", names.get(0));
    assertEquals("test1", names.get(1));

    assertEquals("test", cat.getTable(db, 42).getName());
    assertEquals("test1", cat.getTable(db, 43).getName());
    assertNull(cat.getTable(db, 44));

    helper.addTable(130, 44, "other");
    helper.setSchemaVersion(667);

    ReflectionWrapper wrapper = new ReflectionWrapper(cat);
    wrapper.call("reloadCache");

    tables = cat.listTables(db);
    names = tables.stream().map(TiTableInfo::getName).sorted().collect(Collectors.toList());
    assertEquals(3, tables.size());
    assertEquals("other", names.get(0));
    assertEquals("test", names.get(1));
    assertEquals("test1", names.get(2));

    assertEquals(42, cat.getTable("global_temp", "test").getId());
    assertNull(cat.getTable("global_temp", "test111"));

    helper.dropTable(db.getId(), tables.get(0).getId());
    helper.setSchemaVersion(668);
    wrapper.call("reloadCache");
    tables = cat.listTables(db);
    assertEquals(2, tables.size());

    db = cat.getDatabase("TpCH_001");
    tables = cat.listTables(db);
    assertTrue(tables.isEmpty());
  }
}
