/*
 *
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
 *
 */

package com.pingcap.tikv.region;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.kvproto.Metapb.Peer;
import com.pingcap.tikv.kvproto.Metapb.Region;
import com.pingcap.tikv.util.FastByteComparisons;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class TiRegion implements Serializable {
  private final Region meta;
  private final Set<Long> unreachableStores;
  private Peer peer;
  private final IsolationLevel isolationLevel;
  private final Kvrpcpb.CommandPri commandPri;

  public TiRegion(
      Region meta, Peer peer, IsolationLevel isolationLevel, Kvrpcpb.CommandPri commandPri) {
    Objects.requireNonNull(meta, "meta is null");
    this.meta = decodeRegion(meta);
    if (peer == null || peer.getId() == 0) {
      if (meta.getPeersCount() == 0) {
        throw new TiClientInternalException("Empty peer list for region " + meta.getId());
      }
      this.peer = meta.getPeers(0);
    } else {
      this.peer = peer;
    }
    this.unreachableStores = new HashSet<>();
    this.isolationLevel = isolationLevel;
    this.commandPri = commandPri;
  }

  private Region decodeRegion(Region region) {
    Region.Builder builder =
        Region.newBuilder()
            .setId(region.getId())
            .setRegionEpoch(region.getRegionEpoch())
            .addAllPeers(region.getPeersList());

    if (region.getStartKey().isEmpty()) {
      builder.setStartKey(region.getStartKey());
    } else {
      byte[] decodecStartKey = BytesCodec.readBytes(new CodecDataInput(region.getStartKey()));
      builder.setStartKey(ByteString.copyFrom(decodecStartKey));
    }

    if (region.getEndKey().isEmpty()) {
      builder.setEndKey(region.getEndKey());
    } else {
      byte[] decodecEndKey = BytesCodec.readBytes(new CodecDataInput(region.getEndKey()));
      builder.setEndKey(ByteString.copyFrom(decodecEndKey));
    }

    return builder.build();
  }

  public Peer getLeader() {
    return peer;
  }

  public List<Peer> getLearnerList() {
    int peerCnt = getMeta().getPeersCount();
    List<Peer> peers = new ArrayList<Peer>();
    for (Peer peer : getMeta().getPeersList()) {
      if (peer.getIsLearner()) {
        peers.add(peer);
      }
    }
    return peers;
  }

  public long getId() {
    return this.meta.getId();
  }

  public ByteString getStartKey() {
    return meta.getStartKey();
  }

  public ByteString getEndKey() {
    return meta.getEndKey();
  }

  public Kvrpcpb.Context getContext() {
    Kvrpcpb.Context.Builder builder = Kvrpcpb.Context.newBuilder();
    builder.setIsolationLevel(this.isolationLevel);
    builder.setPriority(this.commandPri);
    builder.setRegionId(meta.getId()).setPeer(this.peer).setRegionEpoch(this.meta.getRegionEpoch());
    return builder.build();
  }

  public class RegionVerID {
    public final long id;
    public final long confVer;
    public final long ver;

    public RegionVerID(long id, long confVer, long ver) {
      this.id = id;
      this.confVer = confVer;
      this.ver = ver;
    }
  }

  // getVerID returns the Region's RegionVerID.
  public RegionVerID getVerID() {
    return new RegionVerID(
        meta.getId(), meta.getRegionEpoch().getConfVer(), meta.getRegionEpoch().getVersion());
  }

  /**
   * switches current peer to the one on specific store. It return false if no peer matches the
   * storeID.
   *
   * @param leaderStoreID is leader peer id.
   * @return false if no peers matches the store id.
   */
  public boolean switchPeer(long leaderStoreID) {
    List<Peer> peers = meta.getPeersList();
    for (Peer p : peers) {
      if (p.getStoreId() == leaderStoreID) {
        this.peer = p;
        return true;
      }
    }
    return false;
  }

  public boolean contains(ByteString key) {
    return (FastByteComparisons.compareTo(
                meta.getStartKey().toByteArray(),
                0,
                meta.getStartKey().size(),
                key.toByteArray(),
                0,
                key.size())
            <= 0)
        && (meta.getEndKey().isEmpty()
            || FastByteComparisons.compareTo(
                    meta.getEndKey().toByteArray(),
                    0,
                    meta.getEndKey().size(),
                    key.toByteArray(),
                    0,
                    key.size())
                > 0);
  }

  public boolean isValid() {
    return peer != null && meta != null;
  }

  public Metapb.RegionEpoch getRegionEpoch() {
    return this.meta.getRegionEpoch();
  }

  public Region getMeta() {
    return meta;
  }

  public String toString() {
    return String.format(
        "{Region[%d] ConfVer[%d] Version[%d] Store[%d] KeyRange[%s]:[%s]}",
        getId(),
        getRegionEpoch().getConfVer(),
        getRegionEpoch().getVersion(),
        getLeader().getStoreId(),
        KeyUtils.formatBytes(getStartKey()),
        KeyUtils.formatBytes(getEndKey()));
  }
}
