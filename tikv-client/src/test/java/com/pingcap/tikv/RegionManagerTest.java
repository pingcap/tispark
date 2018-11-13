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

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.Metapb.StoreState;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.Pair;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RegionManagerTest {
  private PDMockServer server;
  private static final long CLUSTER_ID = 1024;
  private static final String LOCAL_ADDR = "127.0.0.1";
  private RegionManager mgr;

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

    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:" + server.port);
    TiSession session = TiSession.create(conf);
    mgr = session.getRegionManager();
  }

  @After
  public void tearDown() {
    server.stop();
  }

  @Test
  public void getRegionByKey() throws Exception {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});
    ByteString searchKey = ByteString.copyFrom(new byte[] {5});
    ByteString searchKeyNotExists = ByteString.copyFrom(new byte[] {11});
    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    server.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            server.getClusterId(),
            GrpcUtils.makeRegion(
                regionId,
                GrpcUtils.encodeKey(startKey.toByteArray()),
                GrpcUtils.encodeKey(endKey.toByteArray()),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    TiRegion region = mgr.getRegionByKey(startKey);
    assertEquals(region.getId(), regionId);

    TiRegion regionToSearch = mgr.getRegionByKey(searchKey);
    assertEquals(region, regionToSearch);

    // This will in turn invoke rpc and results in an error
    // since we set just one rpc response
    try {
      mgr.getRegionByKey(searchKeyNotExists);
      fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void getStoreByKey() throws Exception {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});
    ByteString searchKey = ByteString.copyFrom(new byte[] {5});
    String testAddress = "testAddress";
    long storeId = 233;
    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    server.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            server.getClusterId(),
            GrpcUtils.makeRegion(
                regionId,
                GrpcUtils.encodeKey(startKey.toByteArray()),
                GrpcUtils.encodeKey(endKey.toByteArray()),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(storeId, 10),
                GrpcUtils.makePeer(storeId + 1, 20))));
    server.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            server.getClusterId(),
            GrpcUtils.makeStore(
                storeId,
                testAddress,
                Metapb.StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    Pair<TiRegion, Store> pair = mgr.getRegionStorePairByKey(searchKey);
    assertEquals(pair.first.getId(), regionId);
    assertEquals(pair.first.getId(), storeId);
  }

  @Test
  public void getRegionById() throws Exception {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});

    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    server.addGetRegionByIDResp(
        GrpcUtils.makeGetRegionResponse(
            server.getClusterId(),
            GrpcUtils.makeRegion(
                regionId,
                GrpcUtils.encodeKey(startKey.toByteArray()),
                GrpcUtils.encodeKey(endKey.toByteArray()),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    TiRegion region = mgr.getRegionById(regionId);
    assertEquals(region.getId(), regionId);

    TiRegion regionToSearch = mgr.getRegionById(regionId);
    assertEquals(region, regionToSearch);

    mgr.invalidateRegion(regionId);

    // This will in turn invoke rpc and results in an error
    // since we set just one rpc response
    try {
      mgr.getRegionById(regionId);
      fail();
    } catch (Exception ignored) {
    }
  }

  @Test
  public void getStoreById() throws Exception {
    long storeId = 234;
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
    Store store = mgr.getStoreById(storeId);
    assertEquals(store.getId(), storeId);

    server.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            server.getClusterId(),
            GrpcUtils.makeStore(
                storeId + 1,
                testAddress,
                StoreState.Tombstone,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    assertNull(mgr.getStoreById(storeId + 1));

    mgr.invalidateStore(storeId);
    try {
      mgr.getStoreById(storeId);
      fail();
    } catch (Exception e) {
    }
  }
}
