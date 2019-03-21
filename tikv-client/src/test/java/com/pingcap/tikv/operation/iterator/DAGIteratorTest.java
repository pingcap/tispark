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
import com.pingcap.tikv.PDMockServer;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
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
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import org.tikv.kvproto.Metapb;

public class DAGIteratorTest {
  private KVMockServer server;
  private PDMockServer pdServer;
  private static final String LOCAL_ADDR = "127.0.0.1";
  private static final long CLUSTER_ID = 1024;
  private int port;
  private TiSession session;
  private TiRegion region;

  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", StringType.VARCHAR)
        .build();
  }

  @Before
  public void setUp() throws Exception {
    pdServer = new PDMockServer();
    pdServer.start(CLUSTER_ID);
    pdServer.addGetMemberResp(
        GrpcUtils.makeGetMembersResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeMember(1, "http://" + LOCAL_ADDR + ":" + pdServer.port),
            GrpcUtils.makeMember(2, "http://" + LOCAL_ADDR + ":" + (pdServer.port + 1)),
            GrpcUtils.makeMember(2, "http://" + LOCAL_ADDR + ":" + (pdServer.port + 2))));

    Metapb.Region r =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
            .setId(233)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.EMPTY)
            .addPeers(Metapb.Peer.newBuilder().setId(11).setStoreId(13))
            .build();

    region = new TiRegion(r, r.getPeers(0), IsolationLevel.RC, CommandPri.Low);
    server = new KVMockServer();
    port = server.start(region);
    // No PD needed in this test
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:" + pdServer.port);
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() {
    server.stop();
  }

  @Test
  public void staleEpochTest() {
    Metapb.Store store =
        Metapb.Store.newBuilder()
            .setAddress(LOCAL_ADDR + ":" + port)
            .setId(1)
            .setState(Metapb.StoreState.Up)
            .build();

    TiTableInfo table = createTable();
    TiDAGRequest req = new TiDAGRequest(PushDownType.NORMAL);
    req.setTableInfo(table);
    req.addRequiredColumn(ColumnRef.create("c1"));
    req.addRequiredColumn(ColumnRef.create("c2"));
    req.setStartTs(new TiTimestamp(0, 1));
    req.resolve();

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
    CoprocessIterator<Row> iter = CoprocessIterator.getRowIterator(req, tasks, session);
    iter.hasNext();
    Row r = iter.next();
    SchemaInfer infer = SchemaInfer.create(req);
    assertEquals(r.get(0, infer.getType(0)), 666L);
    assertEquals(r.get(1, infer.getType(1)), "value1");
  }

  private static KeyRange createByteStringRange(ByteString sKey, ByteString eKey) {
    return KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
  }
}
