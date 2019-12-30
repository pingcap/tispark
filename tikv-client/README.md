## Ti-Client in Java [Under Construction]

A Java client for [TiDB](https://github.com/pingcap/tidb)/[TiKV](https://github.com/pingcap/tikv).
It is supposed to:
+ Communicate via [gRPC](http://www.grpc.io/)
+ Talk to Placement Driver searching for a region
+ Talk to TiKV for reading/writing data and the resulted data is encoded/decoded just like what we do in TiDB.
+ Talk to Coprocessor for calculation push down

## How to build

The following command can install dependencies for you.
```
mvn package
```

this project is designed to hook with `pd` and `tikv` which you can find in `PingCap` github page.

When you work with this project, you have to communicate with `pd` and `tikv`. There is a script taking care of this. By executing the following commands, `pd` and `tikv` can be executed on background.
```
cd scripts
make pd
make tikv
```

## How to use for now
It is not recommended to use tikv java client directly; it is better to use together with `TiSpark`

```java
// Init tidb cluster configuration
TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
TiSession session = TiSession.getInstance(conf);
Catalog cat = session.getCatalog();
TiDBInfo db = cat.getDatabase("tpch_test");
TiTableInfo table = cat.getTable(db, "customer");
Snapshot snapshot = session.createSnapshot();
 
// Generate select ranges
ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MIN_VALUE);
ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MAX_VALUE);
Coprocessor.KeyRange keyRange = Coprocessor.KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
List<Coprocessor.KeyRange> ranges = new ArrayList<>();
ranges.add(keyRange);

 
// Create select request
TiDAGRequest dagRequest = new TiDAGRequest();
dagRequest.addRanges(ranges);
dagRequest.addField(TiColumnRef.create("c_mktsegment", table));
dagRequest.setTableInfo(table);
dagRequest.setStartTs(session.getTimestamp().getVersion());
dagRequest.addWhere(new GreaterEqual(TiConstant.create(5, IntegerType.BIGINT), TiConstant.create(5, IntegerType.BIGINT)));
dagRequest.addGroupByItem(TiByItem.create(TiColumnRef.create("c_mktsegment", table), false));
dagRequest.setLimit(10);
dagRequest.resolve();
 
// Fetch data
Iterator<Row> iterator = snapshot.tableReadRow(dagRequest, table.getId());
System.out.println("Show result:");
while (iterator.hasNext()) {
  Row rowData = iterator.next();
  for (int i = 0; i < rowData.fieldCount(); i++) {
    System.out.print(rowData.get(i, null) + "\t");
  }
  System.out.println();
}

```
Result:
```java
Show result:
BUILDING	
AUTOMOBILE	
MACHINERY	
HOUSEHOLD	
FURNITURE	
```

## TODO
Contributions are welcomed. Here is a [TODO](https://github.com/pingcap/tikv-client-java/wiki/TODO-Lists) and you might contact maxiaoyu@pingcap.com if needed.

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
