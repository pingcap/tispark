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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.util.Pair;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.StoreState;
import org.tikv.shade.com.google.protobuf.ByteString;

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
    String testAddress = "127.0.0.1";

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
    AtomicInteger i = new AtomicInteger(0);
    long[] ids = new long[] {10, 20};
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                ids[i.getAndIncrement()],
                testAddress,
                StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                ids[i.getAndIncrement()],
                testAddress,
                StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));

    TiRegion region = mgr.getRegionByKey(startKey);
    assertEquals(region.getId(), regionId);

    TiRegion regionToSearch = mgr.getRegionByKey(searchKey);
    assertEquals(region, regionToSearch);

    // because use upstream regionManager, so if the key not exists, return null
    TiRegion errValue = mgr.getRegionByKey(searchKeyNotExists);
    assertNull(errValue);
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
    AtomicInteger i = new AtomicInteger(0);
    long[] ids = new long[] {10, 20};
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                ids[i.getAndIncrement()],
                testAddress,
                StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                ids[i.getAndIncrement()],
                testAddress,
                StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    Pair<TiRegion, TiStore> pair = new Pair<>(mgr.getRegionStorePairByKey(searchKey));
    assertEquals(pair.first.getId(), regionId);
    assertEquals(pair.first.getId(), storeId);
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
    TiStore store = mgr.getStoreById(storeId);
    assertEquals(store.getStore().getId(), storeId);

    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                storeId + 1,
                testAddress,
                StoreState.Tombstone,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));

    // because use upstream regionManager, so if the id not exists, throw exception
    try {
      mgr.getStoreById(storeId + 1);
      fail();
    } catch (Exception ignored) {
    }

    mgr.invalidateStore(storeId);
    try {
      mgr.getStoreById(storeId);
      fail();
    } catch (Exception ignored) {
    }
  }
}
