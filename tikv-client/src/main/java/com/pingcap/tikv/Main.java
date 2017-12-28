package com.pingcap.tikv;


import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.aggregate.Max;
import com.pingcap.tikv.expression.aggregate.Min;
import com.pingcap.tikv.expression.aggregate.Sum;

import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.row.Row;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;


public class Main {
  private static final Logger logger = Logger.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    Catalog cat = session.getCatalog();
    TiDBInfo db = cat.getDatabase("tispark_test");
    TiTableInfo table = cat.getTable(db, "full_data_type_table");
    Snapshot snapshot = session.createSnapshot();
    TiDAGRequest selectRequest = new TiDAGRequest(PushDownType.NORMAL);
    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(new ArrayList<>(), table);
    TiColumnRef col = TiColumnRef.create("tp_tinyint", table);
    TiColumnRef col2 = TiColumnRef.create("tp_int", table);
    selectRequest
        .addAggregate(new Sum(col))
        .addAggregate(new Min(col))
        .addAggregate(new Max(col))
        .addRequiredColumn(col)
        .setStartTs(snapshot.getVersion())
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table);
    System.out.println(selectRequest);
    Iterator<Row> it = snapshot.tableRead(selectRequest);
    while (it.hasNext()) {
      Row r = it.next();
      SchemaInfer schemaInfer = SchemaInfer.create(selectRequest);
      for (int i = 0; i < r.fieldCount(); i++) {
        Object val = r.get(i, schemaInfer.getType(i));
        System.out.print(val);
      }
      System.out.println();
    }
    session.close();
  }
}
