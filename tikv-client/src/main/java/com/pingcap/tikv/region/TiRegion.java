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
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.util.FastByteComparisons;
import com.pingcap.tikv.util.KeyRangeUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Region;

public class TiRegion implements Serializable {
  private final Region meta;
  private final IsolationLevel isolationLevel;
  private final Kvrpcpb.CommandPri commandPri;
  private Peer peer;

  public TiRegion(
      Region meta, Peer peer, IsolationLevel isolationLevel, Kvrpcpb.CommandPri commandPri) {
    Objects.requireNonNull(meta, "meta is null");
    this.meta = decodeRegion(meta);
    if (peer == null || peer.getId() == 0) {
      if (meta.getPeersCount() == 0) {
        throw new TiClientInternalException("Empty peer list for region " + meta.getId());
      }
      // region's first peer is leader.
      this.peer = meta.getPeers(0);
    } else {
      this.peer = peer;
    }
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
      byte[] decodedStartKey = BytesCodec.readBytes(new CodecDataInput(region.getStartKey()));
      builder.setStartKey(ByteString.copyFrom(decodedStartKey));
    }

    if (region.getEndKey().isEmpty()) {
      builder.setEndKey(region.getEndKey());
    } else {
      byte[] decodedEndKey = BytesCodec.readBytes(new CodecDataInput(region.getEndKey()));
      builder.setEndKey(ByteString.copyFrom(decodedEndKey));
    }

    return builder.build();
  }

  public Peer getLeader() {
    return peer;
  }

  public List<Peer> getLearnerList() {
    List<Peer> peers = new ArrayList<>();
    for (Peer peer : getMeta().getPeersList()) {
      if (peer.getRole().equals(Metapb.PeerRole.Learner)) {
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

  public boolean contains(Key key) {
    return KeyRangeUtils.makeRange(this.getStartKey(), this.getEndKey()).contains(key);
  }

  public ByteString getEndKey() {
    return meta.getEndKey();
  }

  public Key getRowEndKey() {
    return Key.toRawKey(getEndKey());
  }

  public Kvrpcpb.Context getContext() {
    return getContext(java.util.Collections.emptySet());
  }

  public Kvrpcpb.Context getContext(Set<Long> resolvedLocks) {
    Kvrpcpb.Context.Builder builder = Kvrpcpb.Context.newBuilder();
    builder.setIsolationLevel(this.isolationLevel);
    builder.setPriority(this.commandPri);
    builder.setRegionId(meta.getId()).setPeer(this.peer).setRegionEpoch(this.meta.getRegionEpoch());
    builder.addAllResolvedLocks(resolvedLocks);
    return builder.build();
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
  boolean switchPeer(long leaderStoreID) {
    List<Peer> peers = meta.getPeersList();
    for (Peer p : peers) {
      if (p.getStoreId() == leaderStoreID) {
        this.peer = p;
        return true;
      }
    }
    return false;
  }

  public boolean isMoreThan(ByteString key) {
    return FastByteComparisons.compareTo(
            meta.getStartKey().toByteArray(),
            0,
            meta.getStartKey().size(),
            key.toByteArray(),
            0,
            key.size())
        > 0;
  }

  public boolean isLessThan(ByteString key) {
    return FastByteComparisons.compareTo(
            meta.getEndKey().toByteArray(),
            0,
            meta.getEndKey().size(),
            key.toByteArray(),
            0,
            key.size())
        <= 0;
  }

  public boolean contains(ByteString key) {
    return !isMoreThan(key) && !isLessThan(key);
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

  @Override
  public boolean equals(final Object another) {
    if (!(another instanceof TiRegion)) {
      return false;
    }
    TiRegion anotherRegion = ((TiRegion) another);
    return anotherRegion.meta.equals(this.meta)
        && anotherRegion.peer.equals(this.peer)
        && anotherRegion.commandPri.equals(this.commandPri)
        && anotherRegion.isolationLevel.equals(this.isolationLevel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, peer, isolationLevel, commandPri);
  }

  @Override
  public String toString() {
    // LogDesensitization: show region key range in log
    return String.format(
        "{Region[%d] ConfVer[%d] Version[%d] Store[%d] KeyRange[%s]:[%s]}",
        getId(),
        getRegionEpoch().getConfVer(),
        getRegionEpoch().getVersion(),
        getLeader().getStoreId(),
        KeyUtils.formatBytesUTF8(getStartKey()),
        KeyUtils.formatBytesUTF8(getEndKey()));
  }

  public class RegionVerID {
    final long id;
    final long confVer;
    final long ver;

    RegionVerID(long id, long confVer, long ver) {
      this.id = id;
      this.confVer = confVer;
      this.ver = ver;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof RegionVerID)) {
        return false;
      }

      RegionVerID that = (RegionVerID) other;
      return id == that.id && confVer == that.confVer && ver == that.ver;
    }

    @Override
    public int hashCode() {
      int hash = Long.hashCode(id);
      hash = hash * 31 + Long.hashCode(confVer);
      hash = hash * 31 + Long.hashCode(ver);
      return hash;
    }
  }
}
