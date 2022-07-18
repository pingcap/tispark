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

package com.pingcap.tikv;

import static com.pingcap.tikv.GrpcUtils.encodeKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Region;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.Metapb.StoreState;
import org.tikv.shade.com.google.common.collect.ImmutableList;
import org.tikv.shade.com.google.protobuf.ByteString;

public class PDClientTest extends PDMockServerTest {

  private static final String LOCAL_ADDR_IPV6 = "[::]";

  @Test
  public void testCreate() throws Exception {
    try (PDClient client = session.getPDClient()) {
      assertEquals(client.getLeaderWrapper().getLeaderInfo(), LOCAL_ADDR + ":" + pdServer.port);
      assertEquals(client.getHeader().getClusterId(), CLUSTER_ID);
    }
  }

  @Test
  public void testSwitchLeader() throws Exception {
    try (PDClient client = session.getPDClient()) {
      client.switchLeader(ImmutableList.of("http://" + LOCAL_ADDR + ":" + (pdServer.port + 1)));
      assertEquals(
          client.getLeaderWrapper().getLeaderInfo(), LOCAL_ADDR + ":" + (pdServer.port + 1));
    }
    tearDown();
    setUp(LOCAL_ADDR_IPV6);
    try (PDClient client = session.getPDClient()) {
      client.switchLeader(
          ImmutableList.of("http://" + LOCAL_ADDR_IPV6 + ":" + (pdServer.port + 2)));
      assertEquals(
          client.getLeaderWrapper().getLeaderInfo(), LOCAL_ADDR_IPV6 + ":" + (pdServer.port + 2));
    }
  }

  @Test
  public void testTso() throws Exception {
    try (PDClient client = session.getPDClient()) {
      TiTimestamp ts = client.getTimestamp(defaultBackOff());
      // Test pdServer is set to generate physical == logical + 1
      assertEquals(ts.getPhysical(), ts.getLogical() + 1);
    }
  }

  @Test
  public void testGetRegionByKey() throws Exception {
    byte[] startKey = new byte[] {1, 0, 2, 4};
    byte[] endKey = new byte[] {1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;
    pdServer.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeRegion(
                1,
                encodeKey(startKey),
                encodeKey(endKey),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      Pair<Region, Peer> rl = client.getRegionByKey(defaultBackOff(), ByteString.EMPTY);
      Metapb.Region r = rl.first;
      Metapb.Peer l = rl.second;
      assertEquals(r.getStartKey(), ByteString.copyFrom(startKey));
      assertEquals(r.getEndKey(), ByteString.copyFrom(endKey));
      assertEquals(r.getRegionEpoch().getConfVer(), confVer);
      assertEquals(r.getRegionEpoch().getVersion(), ver);
      assertEquals(l.getId(), 1);
      assertEquals(l.getStoreId(), 10);
    }
  }

  @Test
  public void testGetRegionById() throws Exception {
    byte[] startKey = new byte[] {1, 0, 2, 4};
    byte[] endKey = new byte[] {1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;

    pdServer.addGetRegionByIDResp(
        GrpcUtils.makeGetRegionResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeRegion(
                1,
                encodeKey(startKey),
                encodeKey(endKey),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      Pair<Metapb.Region, Metapb.Peer> rl = client.getRegionByID(defaultBackOff(), 0);
      Metapb.Region r = rl.first;
      Metapb.Peer l = rl.second;
      assertEquals(r.getStartKey(), ByteString.copyFrom(startKey));
      assertEquals(r.getEndKey(), ByteString.copyFrom(endKey));
      assertEquals(r.getRegionEpoch().getConfVer(), confVer);
      assertEquals(r.getRegionEpoch().getVersion(), ver);
      assertEquals(l.getId(), 1);
      assertEquals(l.getStoreId(), 10);
    }
  }

  @Test
  public void testGetStore() throws Exception {
    long storeId = 1;
    String testAddress = "testAddress";
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                storeId,
                testAddress,
                Metapb.StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    try (PDClient client = session.getPDClient()) {
      Store r = client.getStore(defaultBackOff(), 0);
      assertEquals(r.getId(), storeId);
      assertEquals(r.getAddress(), testAddress);
      assertEquals(r.getState(), Metapb.StoreState.Up);
      assertEquals(r.getLabels(0).getKey(), "k1");
      assertEquals(r.getLabels(1).getKey(), "k2");
      assertEquals(r.getLabels(0).getValue(), "v1");
      assertEquals(r.getLabels(1).getValue(), "v2");

      pdServer.addGetStoreResp(
          GrpcUtils.makeGetStoreResponse(
              pdServer.getClusterId(),
              GrpcUtils.makeStore(storeId, testAddress, Metapb.StoreState.Tombstone)));
      assertEquals(StoreState.Tombstone, client.getStore(defaultBackOff(), 0).getState());
    }
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }

  @Test
  public void testRetryPolicy() throws Exception {
    long storeId = 1024;
    ExecutorService service = Executors.newCachedThreadPool();
    pdServer.addGetStoreResp(null);
    pdServer.addGetStoreResp(null);
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(), GrpcUtils.makeStore(storeId, "", Metapb.StoreState.Up)));
    try (PDClient client = session.getPDClient()) {
      Callable<Store> storeCallable =
          () -> client.getStore(ConcreteBackOffer.newCustomBackOff(5000), 0);
      Future<Store> storeFuture = service.submit(storeCallable);
      try {
        Store r = storeFuture.get(50, TimeUnit.SECONDS);
        assertEquals(r.getId(), storeId);
      } catch (TimeoutException e) {
        fail();
      }

      // Should fail
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);

      pdServer.addGetStoreResp(
          GrpcUtils.makeGetStoreResponse(
              pdServer.getClusterId(), GrpcUtils.makeStore(storeId, "", Metapb.StoreState.Up)));
      try {
        client.getStore(defaultBackOff(), 0);
      } catch (GrpcException e) {
        assertTrue(true);
        return;
      }
      fail();
    }
  }
}
