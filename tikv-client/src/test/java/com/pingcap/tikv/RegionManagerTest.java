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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionManager.RegionStorePair;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.region.TiStoreType;
import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.PeerRole;
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
    RegionStorePair pair = mgr.getRegionTiKVStorePairByKey(searchKey);
    assertEquals(pair.region.getId(), regionId);
    assertEquals(pair.store.getId(), storeId);
  }

  @Test
  public void getStoreRegionCoveredRange() {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});
    KeyRange range =
        KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {5}))
            .setEnd(ByteString.copyFrom(new byte[] {7}))
            .build();
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
    List<RegionStorePair> pairs = mgr.getAllRegionTiKVStorePairsInRange(range);
    assertEquals(pairs.size(), 1);
    assertEquals(pairs.get(0).region.getId(), regionId);
    assertEquals(pairs.get(0).store.getId(), storeId);
  }

  @Test
  public void getStoreRegionWithMultiRegionInRange() {
    long storeId = 233;
    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    KeyRange range =
        KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {1}))
            .setEnd(ByteString.copyFrom(new byte[] {10}))
            .build();

    ByteString startKey0 = ByteString.copyFrom(new byte[] {0});
    ByteString endKey0 = ByteString.copyFrom(new byte[] {1});
    long regionId0 = regionId;
    Metapb.Region region0 =
        GrpcUtils.makeRegion(
            regionId0,
            GrpcUtils.encodeKey(startKey0.toByteArray()),
            GrpcUtils.encodeKey(endKey0.toByteArray()),
            GrpcUtils.makeRegionEpoch(confVer, ver),
            GrpcUtils.makePeer(storeId, 10),
            GrpcUtils.makePeer(storeId + 1, 20));

    ByteString startKey1 = ByteString.copyFrom(new byte[] {1});
    ByteString endKey1 = ByteString.copyFrom(new byte[] {3});
    long regionId1 = regionId + 1;
    Metapb.Region region1 =
        GrpcUtils.makeRegion(
            regionId1,
            GrpcUtils.encodeKey(startKey1.toByteArray()),
            GrpcUtils.encodeKey(endKey1.toByteArray()),
            GrpcUtils.makeRegionEpoch(confVer, ver),
            GrpcUtils.makePeer(storeId, 10),
            GrpcUtils.makePeer(storeId + 1, 20));

    ByteString startKey2 = ByteString.copyFrom(new byte[] {3});
    ByteString endKey2 = ByteString.copyFrom(new byte[] {7});
    long regionId2 = regionId + 2;
    Metapb.Region region2 =
        GrpcUtils.makeRegion(
            regionId2,
            GrpcUtils.encodeKey(startKey2.toByteArray()),
            GrpcUtils.encodeKey(endKey2.toByteArray()),
            GrpcUtils.makeRegionEpoch(confVer, ver),
            GrpcUtils.makePeer(storeId, 10),
            GrpcUtils.makePeer(storeId + 1, 20));

    ByteString startKey3 = ByteString.copyFrom(new byte[] {7});
    ByteString endKey3 = ByteString.copyFrom(new byte[] {10});
    long regionId3 = regionId + 3;
    Metapb.Region region3 =
        GrpcUtils.makeRegion(
            regionId3,
            GrpcUtils.encodeKey(startKey3.toByteArray()),
            GrpcUtils.encodeKey(endKey3.toByteArray()),
            GrpcUtils.makeRegionEpoch(confVer, ver),
            GrpcUtils.makePeer(storeId, 10),
            GrpcUtils.makePeer(storeId + 1, 20));

    ByteString startKey4 = ByteString.copyFrom(new byte[] {10});
    ByteString endKey4 = ByteString.copyFrom(new byte[] {11});
    long regionId4 = regionId + 4;
    Metapb.Region region4 =
        GrpcUtils.makeRegion(
            regionId4,
            GrpcUtils.encodeKey(startKey4.toByteArray()),
            GrpcUtils.encodeKey(endKey4.toByteArray()),
            GrpcUtils.makeRegionEpoch(confVer, ver),
            GrpcUtils.makePeer(storeId, 10),
            GrpcUtils.makePeer(storeId + 1, 20));

    String testAddress = "testAddress";
    pdServer.addGetRegionResp(GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region0));

    pdServer.addGetRegionResp(GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region1));

    pdServer.addGetRegionResp(GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region2));

    pdServer.addGetRegionResp(GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region3));

    pdServer.addGetRegionResp(GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region4));

    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                storeId,
                testAddress,
                Metapb.StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    List<RegionStorePair> pairs = mgr.getAllRegionTiKVStorePairsInRange(range);

    assertEquals(pairs.size(), 3);

    RegionStorePair regionStorePair1 = pairs.get(0);
    assertEquals(regionStorePair1.region.getId(), regionId1);
    assertEquals(regionStorePair1.store.getId(), storeId);

    RegionStorePair regionStorePair2 = pairs.get(1);
    assertEquals(regionStorePair2.region.getId(), regionId2);
    assertEquals(pairs.get(0).store.getId(), storeId);

    RegionStorePair regionStorePair3 = pairs.get(2);
    assertEquals(regionStorePair3.region.getId(), regionId3);
    assertEquals(regionStorePair3.store.getId(), storeId);
  }

  @Test
  public void getStoreRegionWithTwoRegionInRange() {
    long storeId = 233;
    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    KeyRange range =
        KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {1}))
            .setEnd(ByteString.copyFrom(new byte[] {10}))
            .build();

    ByteString startKey0 = ByteString.copyFrom(new byte[] {0});
    ByteString endKey0 = ByteString.copyFrom(new byte[] {5});
    long regionId0 = regionId;
    Metapb.Region region0 =
        GrpcUtils.makeRegion(
            regionId0,
            GrpcUtils.encodeKey(startKey0.toByteArray()),
            GrpcUtils.encodeKey(endKey0.toByteArray()),
            GrpcUtils.makeRegionEpoch(confVer, ver),
            GrpcUtils.makePeer(storeId, 10),
            GrpcUtils.makePeer(storeId + 1, 20));

    ByteString startKey1 = ByteString.copyFrom(new byte[] {5});
    ByteString endKey1 = ByteString.copyFrom(new byte[] {11});
    long regionId1 = regionId + 1;
    Metapb.Region region1 =
        GrpcUtils.makeRegion(
            regionId1,
            GrpcUtils.encodeKey(startKey1.toByteArray()),
            GrpcUtils.encodeKey(endKey1.toByteArray()),
            GrpcUtils.makeRegionEpoch(confVer, ver),
            GrpcUtils.makePeer(storeId, 10),
            GrpcUtils.makePeer(storeId + 1, 20));

    String testAddress = "testAddress";
    pdServer.addGetRegionResp(GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region0));

    pdServer.addGetRegionResp(GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region1));

    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                storeId,
                testAddress,
                Metapb.StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    List<RegionStorePair> pairs = mgr.getAllRegionTiKVStorePairsInRange(range);

    assertEquals(pairs.size(), 2);

    RegionStorePair regionStorePair0 = pairs.get(0);
    assertEquals(regionStorePair0.region.getId(), regionId0);
    assertEquals(regionStorePair0.store.getId(), storeId);

    RegionStorePair regionStorePair1 = pairs.get(1);
    assertEquals(regionStorePair1.region.getId(), regionId1);
    assertEquals(regionStorePair1.store.getId(), storeId);
  }

  @Test
  public void getStoreInRegionByStoreType() {
    int leaderStoreId = 1;
    int learnerTiVKStoreId = 2;
    int learnerTiFlashStoreId = 3;
    Peer leader =
        Peer.newBuilder().setId(1).setRole(PeerRole.Voter).setStoreId(leaderStoreId).build();
    Peer learnerTiKV =
        Peer.newBuilder().setId(2).setRole(PeerRole.Learner).setStoreId(learnerTiVKStoreId).build();
    Peer learnerTiFlash =
        Peer.newBuilder()
            .setId(3)
            .setRole(PeerRole.Learner)
            .setStoreId(learnerTiFlashStoreId)
            .build();
    Metapb.Region region =
        Metapb.Region.newBuilder()
            .setStartKey(GrpcUtils.encodeKey(new byte[] {0}))
            .setEndKey(GrpcUtils.encodeKey(new byte[] {5}))
            .addPeers(leader)
            .addPeers(learnerTiKV)
            .addPeers(learnerTiFlash)
            .build();
    TiRegion tiRegion = new TiRegion(region, leader, IsolationLevel.SI, CommandPri.Low);
    String testAddress = "testAddress";
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                leaderStoreId, testAddress, StoreState.Up, TiStoreType.TiKV.getStoreLable())));
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                learnerTiVKStoreId, testAddress, StoreState.Up, TiStoreType.TiKV.getStoreLable())));
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                learnerTiVKStoreId,
                testAddress,
                StoreState.Up,
                TiStoreType.TiFlash.getStoreLable())));
    assertEquals(
        mgr.getStoreInRegionByStoreType(tiRegion, TiStoreType.TiKV).getId(), leaderStoreId);
    assertEquals(
        mgr.getStoreInRegionByStoreType(tiRegion, TiStoreType.TiKV).getId(), leaderStoreId);
  }

  @Test
  public void getStoreInRegionByStoreTypeExpectException() {
    int leaderStoreId = 1;
    int learnerTiVKStoreId = 2;
    int learnerTiFlashStoreId = 3;
    Peer leader =
        Peer.newBuilder().setId(1).setRole(PeerRole.Voter).setStoreId(leaderStoreId).build();
    Peer learnerTiKV =
        Peer.newBuilder().setId(2).setRole(PeerRole.Learner).setStoreId(learnerTiVKStoreId).build();
    Peer learnerTiFlash =
        Peer.newBuilder()
            .setId(3)
            .setRole(PeerRole.Learner)
            .setStoreId(learnerTiFlashStoreId)
            .build();
    Metapb.Region region =
        Metapb.Region.newBuilder()
            .setStartKey(GrpcUtils.encodeKey(new byte[] {0}))
            .setEndKey(GrpcUtils.encodeKey(new byte[] {5}))
            .addPeers(leader)
            .addPeers(learnerTiKV)
            .addPeers(learnerTiFlash)
            .build();
    TiRegion tiRegion = new TiRegion(region, leader, IsolationLevel.SI, CommandPri.Low);
    String testAddress = "testAddress";
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                leaderStoreId, testAddress, StoreState.Up, TiStoreType.TiKV.getStoreLable())));
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                learnerTiVKStoreId, testAddress, StoreState.Up, TiStoreType.TiKV.getStoreLable())));
    try {
      mgr.getStoreInRegionByStoreType(tiRegion, TiStoreType.TiFlash);
      fail();
    } catch (TiClientInternalException e) {

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
