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
package com.pingcap.tikv.example;


import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.ScanAnalyzer;
import com.pingcap.tikv.row.Row;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class IndexScanExample {
  private final TiSession session;
  private final TiTableInfo table;

  public IndexScanExample(String pdAddr, String dbName, String tableName) {
    TiConfiguration conf = TiConfiguration.createDefault(pdAddr);
    session = TiSession.create(conf);
    Catalog cat = session.getCatalog();
    TiDBInfo db = cat.getDatabase("tispark_test");
    table = cat.getTable(db, "full_data_type_table");
  }

  public void run(String indexName, List<ColumnRef> columns, List<Expression> filters) {
    Snapshot snapshot = session.createSnapshot();

    TiDAGRequest dagRequest = new TiDAGRequest(PushDownType.NORMAL);
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    columns.forEach(c -> c.resolve(table));
    columns.forEach(dagRequest::addRequiredColumn);

    TiIndexInfo selectedIndex = null;
    for (TiIndexInfo index : table.getIndices()) {
      if (index.getName().equals(indexName)) {
        selectedIndex = index;
        break;
      }
    }
    Objects.requireNonNull(selectedIndex, "Provided Index Name " + indexName + " does not exist.");
    ScanAnalyzer builder = new ScanAnalyzer();
    ScanAnalyzer.ScanPlan scanPlan = builder.buildScan(columns, filters, selectedIndex, table);
    dagRequest.addRanges(scanPlan.getKeyRanges());
    dagRequest.setIndexInfo(scanPlan.getIndex());
    dagRequest.setIsDoubleRead(true);
    Iterator<Row> iter = snapshot.tableRead(dagRequest);
    SchemaInfer schemaInfer = SchemaInfer.create(dagRequest);

    while (iter.hasNext()) {
      Row row = iter.next();
      for (int i = 0; i < row.fieldCount(); i++) {
        Object val = row.get(i, schemaInfer.getType(i));
        System.out.print(val);
        System.out.print(" ");
      }
      System.out.print("\n");
    }
    System.out.println("-------------------------------\n");
  }
}
