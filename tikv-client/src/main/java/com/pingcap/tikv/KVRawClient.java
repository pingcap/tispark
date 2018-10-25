package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.operation.iterator.RawScanIterator;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KVRawClient {
  private static final String RAW_PREFIX = "raw_";
  private static final String DEFAULT_PD_ADDRESS = "127.0.0.1:2379";
  private final TiSession session;
  private final RegionManager regionManager;

  public KVRawClient(String addresses) {
    session = TiSession.create(TiConfiguration.createDefault(addresses));
    regionManager = session.getRegionManager();
  }

  public KVRawClient() {
    this(DEFAULT_PD_ADDRESS);
  }

  public void rawPut(ByteString key, ByteString value) {
    Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByRawKey(key);
    RegionStoreClient client =
        RegionStoreClient.create(pair.first, pair.second, session);
    client.rawPut(defaultBackOff(), key, value);
  }

  public ByteString rawGet(ByteString key) {
    Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByRawKey(key);
    RegionStoreClient client =
        RegionStoreClient.create(pair.first, pair.second, session);
    return client.rawGet(defaultBackOff(), key);
  }

  public void rawDelete(ByteString key) {
    TiRegion region = regionManager.getRegionByRawKey(key);
    Kvrpcpb.Context context =
        Kvrpcpb.Context.newBuilder()
            .setRegionId(region.getId())
            .setRegionEpoch(region.getRegionEpoch())
            .setPeer(region.getLeader())
            .build();
    Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByRawKey(key);
    RegionStoreClient client =
        RegionStoreClient.create(pair.first, pair.second, session);
    client.rawDelete(defaultBackOff(), key, context);
  }

  public List<Kvrpcpb.KvPair> rawScan(ByteString startKey, ByteString endKey) {
    Iterator<Kvrpcpb.KvPair> iterator = rawScanIterator(startKey, endKey);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  public void rawPutUtf8(String key, String value) {
    rawPut(rawKey(key), rawValue(value));
  }

  public ByteString rawGetUtf8(String key) {
    return rawGet(rawKey(key));
  }

  public void rawDeleteUtf8(String key) {
    rawDelete(rawKey(key));
  }

  public List<Kvrpcpb.KvPair> rawScanUtf8(String startKey, String endKey) {
    return rawScan(rawKey(startKey), rawKey(endKey));
  }

  private Iterator<Kvrpcpb.KvPair> rawScanIterator(ByteString startKey, ByteString endKey) {
    return new RawScanIterator(startKey, endKey, session);
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }

  static ByteString rawKey(String key) {
    return ByteString.copyFromUtf8(RAW_PREFIX + key);
  }

  static ByteString rawValue(String value) {
    return ByteString.copyFromUtf8(value);
  }
}
