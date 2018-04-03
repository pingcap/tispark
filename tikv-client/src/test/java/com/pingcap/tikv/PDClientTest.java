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

package com.pingcap.tikv;

import static com.pingcap.tikv.GrpcUtils.encodeKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.Metapb.StoreState;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.ZeroBackOff;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PDClientTest {

  private PDMockServer server;
  private static final long CLUSTER_ID = 1024;
  private static final String LOCAL_ADDR = "127.0.0.1";
  private static TiSession session;

  @Before
  public void setup() throws IOException {
    server = new PDMockServer();
    server.start(CLUSTER_ID);
    server.addGetMemberResp(
        GrpcUtils.makeGetMembersResponse(
            server.getClusterId(),
            GrpcUtils.makeMember(1, "http://" + LOCAL_ADDR + ":" + server.port),
            GrpcUtils.makeMember(2, "http://" + LOCAL_ADDR + ":" + (server.port + 1)),
            GrpcUtils.makeMember(2, "http://" + LOCAL_ADDR + ":" + (server.port + 2))));
    TiConfiguration conf =
        TiConfiguration.createDefault("127.0.0.1:" + server.port);
    conf.setRetryTimeMs(3000);
    conf.setBackOffClass(ConcreteBackOffer.class);
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() {
    server.stop();
  }

  @Test
  public void testCreate() throws Exception {
    try (PDClient client = session.getPDClient()) {
      assertEquals(
          client.getLeaderWrapper().getLeaderInfo(),
          LOCAL_ADDR + ":" + server.port);
      assertEquals(client.getHeader().getClusterId(), CLUSTER_ID);
    }
  }

  @Test
  public void testTso() throws Exception {
    try (PDClient client = session.getPDClient()) {
      TiTimestamp ts = client.getTimestamp();
      // Test server is set to generate physical == logical + 1
      assertEquals(ts.getPhysical(), ts.getLogical() + 1);
    }
  }

  @Test
  public void testGetRegionByKey() throws Exception {
    byte[] startKey = new byte[] {1, 0, 2, 4};
    byte[] endKey = new byte[] {1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;
    server.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            server.getClusterId(),
            GrpcUtils.makeRegion(
                1,
                encodeKey(startKey),
                encodeKey(endKey),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      TiRegion r = client.getRegionByKey(ByteString.EMPTY);
      assertEquals(r.getStartKey(), ByteString.copyFrom(startKey));
      assertEquals(r.getEndKey(), ByteString.copyFrom(endKey));
      assertEquals(r.getRegionEpoch().getConfVer(), confVer);
      assertEquals(r.getRegionEpoch().getVersion(), ver);
      assertEquals(r.getLeader().getId(), 1);
      assertEquals(r.getLeader().getStoreId(), 10);
    }
  }

  @Test
  public void testGetRegionByKeyAsync() throws Exception {
    byte[] startKey = new byte[] {1, 0, 2, 4};
    byte[] endKey = new byte[] {1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;
    server.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            server.getClusterId(),
            GrpcUtils.makeRegion(
                1,
                encodeKey(startKey),
                encodeKey(endKey),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      TiRegion r = client.getRegionByKeyAsync(ByteString.EMPTY).get();
      assertEquals(r.getStartKey(), ByteString.copyFrom(startKey));
      assertEquals(r.getEndKey(), ByteString.copyFrom(endKey));
      assertEquals(r.getRegionEpoch().getConfVer(), confVer);
      assertEquals(r.getRegionEpoch().getVersion(), ver);
      assertEquals(r.getLeader().getId(), 1);
      assertEquals(r.getLeader().getStoreId(), 10);
    }
  }

  @Test
  public void testGetRegionById() throws Exception {
    byte[] startKey = new byte[] {1, 0, 2, 4};
    byte[] endKey = new byte[] {1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;

    server.addGetRegionByIDResp(
        GrpcUtils.makeGetRegionResponse(
            server.getClusterId(),
            GrpcUtils.makeRegion(
                1,
                encodeKey(startKey),
                encodeKey(endKey),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      TiRegion r = client.getRegionByID(0);
      assertEquals(r.getStartKey(), ByteString.copyFrom(startKey));
      assertEquals(r.getEndKey(), ByteString.copyFrom(endKey));
      assertEquals(r.getRegionEpoch().getConfVer(), confVer);
      assertEquals(r.getRegionEpoch().getVersion(), ver);
      assertEquals(r.getLeader().getId(), 1);
      assertEquals(r.getLeader().getStoreId(), 10);
    }
  }

  @Test
  public void testGetRegionByIdAsync() throws Exception {
    byte[] startKey = new byte[] {1, 0, 2, 4};
    byte[] endKey = new byte[] {1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;
    server.addGetRegionByIDResp(
        GrpcUtils.makeGetRegionResponse(
            server.getClusterId(),
            GrpcUtils.makeRegion(
                1,
                encodeKey(startKey),
                encodeKey(endKey),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      TiRegion r = client.getRegionByIDAsync(0).get();
      assertEquals(r.getStartKey(), ByteString.copyFrom(startKey));
      assertEquals(r.getEndKey(), ByteString.copyFrom(endKey));
      assertEquals(r.getRegionEpoch().getConfVer(), confVer);
      assertEquals(r.getRegionEpoch().getVersion(), ver);
      assertEquals(r.getLeader().getId(), 1);
      assertEquals(r.getLeader().getStoreId(), 10);
    }
  }

  @Test
  public void testGetStore() throws Exception {
    long storeId = 1;
    String testAddress = "testAddress";
    server.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            server.getClusterId(),
            GrpcUtils.makeStore(
                storeId,
                testAddress,
                Metapb.StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    try (PDClient client = session.getPDClient()) {
      Store r = client.getStore(0);
      assertEquals(r.getId(), storeId);
      assertEquals(r.getAddress(), testAddress);
      assertEquals(r.getState(), Metapb.StoreState.Up);
      assertEquals(r.getLabels(0).getKey(), "k1");
      assertEquals(r.getLabels(1).getKey(), "k2");
      assertEquals(r.getLabels(0).getValue(), "v1");
      assertEquals(r.getLabels(1).getValue(), "v2");

      server.addGetStoreResp(
          GrpcUtils.makeGetStoreResponse(
              server.getClusterId(),
              GrpcUtils.makeStore(storeId, testAddress, Metapb.StoreState.Tombstone)));
      assertEquals(StoreState.Tombstone, client.getStore(0).getState());
    }
  }

  @Test
  public void testGetStoreAsync() throws Exception {
    long storeId = 1;
    String testAddress = "testAddress";
    server.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            server.getClusterId(),
            GrpcUtils.makeStore(
                storeId,
                testAddress,
                Metapb.StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    try (PDClient client = session.getPDClient()) {
      Store r = client.getStoreAsync(0).get();
      assertEquals(r.getId(), storeId);
      assertEquals(r.getAddress(), testAddress);
      assertEquals(r.getState(), Metapb.StoreState.Up);
      assertEquals(r.getLabels(0).getKey(), "k1");
      assertEquals(r.getLabels(1).getKey(), "k2");
      assertEquals(r.getLabels(0).getValue(), "v1");
      assertEquals(r.getLabels(1).getValue(), "v2");

      server.addGetStoreResp(
          GrpcUtils.makeGetStoreResponse(
              server.getClusterId(),
              GrpcUtils.makeStore(storeId, testAddress, Metapb.StoreState.Tombstone)));
      assertEquals(StoreState.Tombstone, client.getStoreAsync(0).get().getState());
    }
  }

  @Test
  public void testRetryPolicy() throws Exception {
//    long storeId = 1024;
//    server.addGetStoreResp(null);
//    server.addGetStoreResp(null);
//    server.addGetStoreResp(
//        GrpcUtils.makeGetStoreResponse(
//            server.getClusterId(), GrpcUtils.makeStore(storeId, "", Metapb.StoreState.Up)));
//    try (PDClient client = session.getPDClient()) {
//      Store r = client.getStore(0);
//      assertEquals(r.getId(), storeId);
//
//      // Should fail
//      server.addGetStoreResp(null);
//      server.addGetStoreResp(null);
//      server.addGetStoreResp(null);
//
//      server.addGetStoreResp(
//          GrpcUtils.makeGetStoreResponse(
//              server.getClusterId(), GrpcUtils.makeStore(storeId, "", Metapb.StoreState.Up)));
//      try {
//        client.getStore(0);
//      } catch (GrpcException e) {
//        assertTrue(true);
//        return;
//      }
//      fail();
//    }
  }
}
