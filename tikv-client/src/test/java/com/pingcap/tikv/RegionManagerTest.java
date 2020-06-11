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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.Pair;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.Metapb.StoreState;

public class RegionManagerTest extends PDMockServerTest {
  private RegionManager mgr;

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    mgr = session.getRegionManager();
  }

  @Test
  public void getRegionByKey() {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});
    ByteString searchKey = ByteString.copyFrom(new byte[] {5});
    ByteString searchKeyNotExists = ByteString.copyFrom(new byte[] {11});
    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    pdServer.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            pdServer.getClusterId(),
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
    } catch (Exception ignored) {
    }
  }

  @Test
  public void getStoreByKey() {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});
    ByteString searchKey = ByteString.copyFrom(new byte[] {5});
    String testAddress = "testAddress";
    long storeId = 233;
    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    pdServer.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeRegion(
                regionId,
                GrpcUtils.encodeKey(startKey.toByteArray()),
                GrpcUtils.encodeKey(endKey.toByteArray()),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(storeId, 10),
                GrpcUtils.makePeer(storeId + 1, 20))));
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
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
  public void getRegionById() {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});

    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    pdServer.addGetRegionByIDResp(
        GrpcUtils.makeGetRegionResponse(
            pdServer.getClusterId(),
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
  public void getStoreById() {
    long storeId = 234;
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
    Store store = mgr.getStoreById(storeId);
    assertEquals(store.getId(), storeId);

    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
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
    } catch (Exception ignored) {
    }
  }
}
