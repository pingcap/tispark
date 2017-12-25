package com.pingcap.tikv;


import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.row.Row;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

public class Main {
  private static final Logger logger = Logger.getLogger(Main.class);
  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    Catalog cat = session.getCatalog();
    TiDBInfo db = cat.getDatabase("test");
    TiTableInfo table = cat.getTable(db, "t1");
    Snapshot snapshot = session.createSnapshot();
    TiDAGRequest selectRequest = new TiDAGRequest(PushDownType.NORMAL);
    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(new ArrayList<>(), table);
    selectRequest.addRequiredColumn(TiColumnRef.create("c1", table))
        .addRequiredColumn(TiColumnRef.create("c2", table))
        .addRequiredColumn(TiColumnRef.create("s1", table))
        .addRequiredColumn(TiColumnRef.create("f1", table))
        .setStartTs(snapshot.getVersion())
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table);
    Iterator<Row> it = snapshot.tableRead(selectRequest);
    while (it.hasNext()) {
      Row r = it.next();
      SchemaInfer schemaInfer = SchemaInfer.create(selectRequest);
      for (int i = 0; i < r.fieldCount(); i++) {
        Object val = r.get(i, schemaInfer.getType(i));
        System.out.print(val);
      }
    }
    session.close();
  }
}
