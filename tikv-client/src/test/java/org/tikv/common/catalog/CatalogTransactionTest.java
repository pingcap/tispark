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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.common.catalog;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.junit.Test;
import org.tikv.common.meta.MetaUtils.MetaMockHelper;
import org.tikv.common.meta.TiDBInfo;
import org.tikv.common.meta.TiTableInfo;

public class CatalogTransactionTest extends CatalogTest {
  @Test
  public void getLatestSchemaVersionTest() {
    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.setSchemaVersion(666);
    CatalogTransaction trx = new CatalogTransaction(session.createSnapshot());
    assertEquals(666, trx.getLatestSchemaVersion());
  }

  @Test
  public void getDatabasesTest() {
    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.addDatabase(130, "global_temp");
    helper.addDatabase(264, "TPCH_001");

    CatalogTransaction trx = new CatalogTransaction(session.createSnapshot());
    List<TiDBInfo> dbs = trx.getDatabases();
    assertEquals(2, dbs.size());
    assertEquals(130, dbs.get(0).getId());
    assertEquals("global_temp", dbs.get(0).getName());

    assertEquals(264, dbs.get(1).getId());
    assertEquals("tpch_001", dbs.get(1).getName());

    TiDBInfo db = trx.getDatabase(130);
    assertEquals(130, db.getId());
    assertEquals("global_temp", db.getName());
  }

  @Test
  public void getMultipleDatabasesTest() {
    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.addDatabase(130, "global_temp");
    for (int i = 1; i <= 200; i++) {
      helper.addDatabase(263 + i, String.format("TPCH_%03d", i));
    }

    CatalogTransaction trx = new CatalogTransaction(session.createSnapshot());
    List<TiDBInfo> dbs = trx.getDatabases();
    assertEquals(201, dbs.size());
    assertEquals(130, dbs.get(0).getId());
    assertEquals("global_temp", dbs.get(0).getName());

    assertEquals(264, dbs.get(1).getId());
    assertEquals("tpch_001", dbs.get(1).getName());

    assertEquals(463, dbs.get(200).getId());
    assertEquals("tpch_200", dbs.get(200).getName());

    TiDBInfo db0 = trx.getDatabase(130);
    assertEquals(130, db0.getId());
    assertEquals("global_temp", db0.getName());

    TiDBInfo db1 = trx.getDatabase(400);
    assertEquals(400, db1.getId());
    assertEquals("tpch_137", db1.getName());
  }

  @Test
  public void getTablesTest() {
    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.addTable(130, 42, "test");
    helper.addTable(130, 43, "test1");

    CatalogTransaction trx = new CatalogTransaction(session.createSnapshot());
    List<TiTableInfo> tables = trx.getTables(130);
    assertEquals(2, tables.size());
    assertEquals("test", tables.get(0).getName());
    assertEquals("test1", tables.get(1).getName());
  }
}
