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

package com.pingcap.tikv.operation.iterator;

import static junit.framework.TestCase.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.GrpcUtils;
import com.pingcap.tikv.KVMockServer;
import com.pingcap.tikv.MockServerTest;
import com.pingcap.tikv.Version;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.List;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Metapb;

public class DAGIteratorTest extends MockServerTest {

  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", StringType.VARCHAR)
        .build();
  }

  private static KeyRange createByteStringRange(ByteString sKey, ByteString eKey) {
    return KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
  }

  @Test
  public void staleEpochTest() {
    Metapb.Store store =
        Metapb.Store.newBuilder()
            .setAddress(LOCAL_ADDR + ":" + port)
            .setId(1)
            .setState(Metapb.StoreState.Up)
            .setVersion(Version.RESOLVE_LOCK_V4)
            .build();

    TiTableInfo table = createTable();
    TiDAGRequest req = new TiDAGRequest(PushDownType.NORMAL);
    req.setTableInfo(table);
    req.addRequiredColumn(ColumnRef.create("c1", IntegerType.INT));
    req.addRequiredColumn(ColumnRef.create("c2", StringType.VARCHAR));
    req.setStartTs(new TiTimestamp(0, 1));

    List<KeyRange> keyRanges =
        ImmutableList.of(
            createByteStringRange(
                ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("key4")));

    pdServer.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region.getMeta()));
    pdServer.addGetStoreResp(GrpcUtils.makeGetStoreResponse(pdServer.getClusterId(), store));
    server.putError("key1", KVMockServer.STALE_EPOCH);
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerCodec.writeLongFully(cdo, 666, false);
    BytesCodec.writeBytesFully(cdo, "value1".getBytes());
    server.put("key1", cdo.toByteString());
    List<RegionTask> tasks = ImmutableList.of(RegionTask.newInstance(region, store, keyRanges));
    CoprocessorIterator<Row> iter = CoprocessorIterator.getRowIterator(req, tasks, session);
    if (!iter.hasNext()) {
      assertEquals("iterator has next should be true", true, false);
    } else {
      Row r = iter.next();
      SchemaInfer infer = SchemaInfer.create(req);
      assertEquals(r.get(0, infer.getType(0)), 666L);
      assertEquals(r.get(1, infer.getType(1)), "value1");
    }
  }
}
